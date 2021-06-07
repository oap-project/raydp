# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.
#
# Description: an implementation of a deep learning recommendation model (DLRM)
# The model input consists of dense and sparse features. The former is a vector
# of floating point values. The latter is a list of sparse indices into
# embedding tables, which consist of vectors of floating point values.
# The selected vectors are passed to mlp networks denoted by triangles,
# in some cases the vectors are interacted through operators (Ops).
#
# output:
#                         vector of values
# model:                        |
#                              /\
#                             /__\
#                               |
#       _____________________> Op  <___________________
#     /                         |                      \
#    /\                        /\                      /\
#   /__\                      /__\           ...      /__\
#    |                          |                       |
#    |                         Op                      Op
#    |                    ____/__\_____           ____/__\____
#    |                   |_Emb_|____|__|    ...  |_Emb_|__|___|
# input:
# [ dense features ]     [sparse indices] , ..., [sparse indices]
#
# More precise definition of model layers:
# 1) fully connected layers of an mlp
# z = f(y)
# y = Wx + b
#
# 2) embedding lookup (for a list of sparse indices p=[p1,...,pk])
# z = Op(e1,...,ek)
# obtain vectors e1=E[:,p1], ..., ek=E[:,pk]
#
# 3) Operator Op can be one of the following
# Sum(e1,...,ek) = e1 + ... + ek
# Dot(e1,...,ek) = [e1'e1, ..., e1'ek, ..., ek'e1, ..., ek'ek]
# Cat(e1,...,ek) = [e1', ..., ek']'
# where ' denotes transpose operation
#
# References:
# [1] Maxim Naumov, Dheevatsa Mudigere, Hao-Jun Michael Shi, Jianyu Huang,
# Narayanan Sundaram, Jongsoo Park, Xiaodong Wang, Udit Gupta, Carole-Jean Wu,
# Alisson G. Azzolini, Dmytro Dzhulgakov, Andrey Mallevich, Ilia Cherniavskii,
# Yinghai Lu, Raghuraman Krishnamoorthi, Ansha Yu, Volodymyr Kondratenko,
# Stephanie Pereira, Xianjie Chen, Wenlin Chen, Vijay Rao, Bill Jia, Liang Xiong,
# Misha Smelyanskiy, "Deep Learning Recommendation Model for Personalization and
# Recommendation Systems", CoRR, arXiv:1906.00091, 2019

from __future__ import absolute_import, division, print_function, unicode_literals

# miscellaneous
import builtins
# import bisect
# import shutil
import time
import json
import sys
# data generation
from dlrm import extend_distributed as ext_dist, mlperf_logger

# numpy
import numpy as np

# onnx
# The onnx import causes deprecation warnings every time workers
# are spawned during testing. So, we filter out those warnings.
import warnings
with warnings.catch_warnings():
    warnings.filterwarnings("ignore", category=DeprecationWarning)
import onnx

# pytorch
import torch
import torch.nn as nn
from torch.nn.parallel.parallel_apply import parallel_apply
from torch.nn.parallel.replicate import replicate
from torch.nn.parallel.scatter_gather import gather, scatter

# For distributed run

import intel_pytorch_extension as ipex
from intel_pytorch_extension import core

# quotient-remainder trick
from dlrm.tricks.qr_embedding_bag import QREmbeddingBag
# mixed-dimension trick
from dlrm.tricks.md_embedding_bag import PrEmbeddingBag, md_solver

import sklearn.metrics

# from torchviz import make_dot
# import torch.nn.functional as Functional
# from torch.nn.parameter import Parameter

from torch.optim.lr_scheduler import _LRScheduler

exc = getattr(builtins, "IOError", "FileNotFoundError")
device = None


class LRPolicyScheduler(_LRScheduler):
    def __init__(self, optimizer, num_warmup_steps, decay_start_step, num_decay_steps):
        self.num_warmup_steps = num_warmup_steps
        self.decay_start_step = decay_start_step
        self.decay_end_step = decay_start_step + num_decay_steps
        self.num_decay_steps = num_decay_steps

        if self.decay_start_step < self.num_warmup_steps:
            sys.exit("Learning rate warmup must finish before the decay starts")

        super(LRPolicyScheduler, self).__init__(optimizer)

    def get_lr(self):
        step_count = self._step_count
        if step_count < self.num_warmup_steps:
            # warmup
            scale = 1.0 - (self.num_warmup_steps - step_count) / self.num_warmup_steps
            lr = [base_lr * scale for base_lr in self.base_lrs]
            self.last_lr = lr
        elif self.decay_start_step <= step_count and step_count < self.decay_end_step:
            # decay
            decayed_steps = step_count - self.decay_start_step
            scale = ((self.num_decay_steps - decayed_steps) / self.num_decay_steps) ** 2
            min_lr = 0.0000001
            lr = [max(min_lr, base_lr * scale) for base_lr in self.base_lrs]
            self.last_lr = lr
        else:
            if self.num_decay_steps > 0:
                # freeze at last, either because we're after decay
                # or because we're between warmup and decay
                lr = self.last_lr
            else:
                # do not adjust
                lr = self.base_lrs
        return lr


class Cast(nn.Module):
    __constants__ = ['to_dtype']

    def __init__(self, to_dtype):
        super(Cast, self).__init__()
        self.to_dtype = to_dtype

    def forward(self, input):
        if input.is_mkldnn:
            return input.to_dense(self.to_dtype)
        else:
            return input.to(self.to_dtype)

    def extra_repr(self):
        return 'to(%s)' % self.to_dtype


### define dlrm in PyTorch ###
class DLRM_Net(nn.Module):
    def create_mlp(self, ln, sigmoid_layer):
        # build MLP layer by layer
        layers = nn.ModuleList()
        for i in range(0, ln.size - 1):
            n = ln[i]
            m = ln[i + 1]

            # construct fully connected operator
            if self.use_ipex and self.bf16:
                LL = ipex.IpexMLPLinear(int(n), int(m), bias=True, output_stays_blocked=(i < ln.size - 2), default_blocking=32)
            else:
                LL = nn.Linear(int(n), int(m), bias=True)

            # initialize the weights
            # with torch.no_grad():
            # custom Xavier input, output or two-sided fill
            mean = 0.0  # std_dev = np.sqrt(variance)
            std_dev = np.sqrt(2 / (m + n))  # np.sqrt(1 / m) # np.sqrt(1 / n)
            W = np.random.normal(mean, std_dev, size=(m, n)).astype(np.float32)
            std_dev = np.sqrt(1 / m)  # np.sqrt(2 / (m + 1))
            bt = np.random.normal(mean, std_dev, size=m).astype(np.float32)
            # approach 1
            LL.weight.data = torch.tensor(W, requires_grad=True)
            LL.bias.data = torch.tensor(bt, requires_grad=True)
            # approach 2
            # LL.weight.data.copy_(torch.tensor(W))
            # LL.bias.data.copy_(torch.tensor(bt))
            # approach 3
            # LL.weight = Parameter(torch.tensor(W),requires_grad=True)
            # LL.bias = Parameter(torch.tensor(bt),requires_grad=True)

            if self.bf16 and ipex.is_available():
                LL.to(torch.bfloat16)
            # prepack weight for IPEX Linear
            if hasattr(LL, 'reset_weight_shape'):
                LL.reset_weight_shape(block_for_dtype=torch.bfloat16)

            layers.append(LL)

            # construct sigmoid or relu operator
            if i == sigmoid_layer:
                if self.bf16:
                    layers.append(Cast(torch.float32))
                layers.append(nn.Sigmoid())
            else:
                if self.use_ipex and self.bf16:
                    LL.set_activation_type('relu')
                else:
                    layers.append(nn.ReLU())

        # approach 1: use ModuleList
        # return layers
        # approach 2: use Sequential container to wrap all layers
        return torch.nn.Sequential(*layers)

    def create_emb(self, m, ln, local_ln_emb_sparse=None, ln_emb_dense=None):
        emb_l = nn.ModuleList()
        # save the numpy random state
        np_rand_state = np.random.get_state()
        emb_dense = nn.ModuleList()
        emb_sparse = nn.ModuleList()
        embs = range(len(ln))
        if local_ln_emb_sparse or ln_emb_dense:
            embs = local_ln_emb_sparse + ln_emb_dense
        for i in embs:
            # Use per table random seed for Embedding initialization
            np.random.seed(self.l_emb_seeds[i])
            n = ln[i]
            # construct embedding operator
            if self.qr_flag and n > self.qr_threshold:
                EE = QREmbeddingBag(n, m, self.qr_collisions,
                                    operation=self.qr_operation, mode="sum", sparse=True)
            elif self.md_flag:
                base = max(m)
                _m = m[i] if n > self.md_threshold else base
                EE = PrEmbeddingBag(n, _m, base)
                # use np initialization as below for consistency...
                W = np.random.uniform(
                    low=-np.sqrt(1 / n), high=np.sqrt(1 / n), size=(n, _m)
                ).astype(np.float32)
                EE.embs.weight.data = torch.tensor(W, requires_grad=True)

            else:
                # initialize embeddings
                # nn.init.uniform_(EE.weight, a=-np.sqrt(1 / n), b=np.sqrt(1 / n))
                W = np.random.uniform(
                    low=-np.sqrt(1 / n), high=np.sqrt(1 / n), size=(n, m)
                ).astype(np.float32)
                # approach 1
                if n >= self.sparse_dense_boundary:
                    EE = nn.EmbeddingBag(n, m, mode="sum", sparse=True, _weight=torch.tensor(W, requires_grad=True))
                else:
                    EE = nn.EmbeddingBag(n, m, mode="sum", sparse=False, _weight=torch.tensor(W, requires_grad=True))
                # approach 2
                # EE.weight.data.copy_(torch.tensor(W))
                # approach 3
                # EE.weight = Parameter(torch.tensor(W),requires_grad=True)
                if self.bf16 and ipex.is_available():
                    EE.to(torch.bfloat16)

            if ext_dist.my_size > 1:
                if n >= self.sparse_dense_boundary:
                    emb_sparse.append(EE)
                else:
                    emb_dense.append(EE)

            emb_l.append(EE)

        # Restore the numpy random state
        np.random.set_state(np_rand_state)
        return emb_l, emb_dense, emb_sparse

    def __init__(
            self,
            m_spa=None,
            ln_emb=None,
            ln_bot=None,
            ln_top=None,
            arch_interaction_op=None,
            arch_interaction_itself=False,
            sigmoid_bot=-1,
            sigmoid_top=-1,
            sync_dense_params=True,
            loss_threshold=0.0,
            ndevices=-1,
            qr_flag=False,
            qr_operation="mult",
            qr_collisions=0,
            qr_threshold=200,
            md_flag=False,
            md_threshold=200,
            bf16=False,
            use_ipex=False,
            sparse_dense_boundary = 2048
    ):
        super(DLRM_Net, self).__init__()

        if (
                (m_spa is not None)
                and (ln_emb is not None)
                and (ln_bot is not None)
                and (ln_top is not None)
                and (arch_interaction_op is not None)
        ):

            # save arguments
            self.ndevices = ndevices
            self.output_d = 0
            self.parallel_model_batch_size = -1
            self.parallel_model_is_not_prepared = True
            self.arch_interaction_op = arch_interaction_op
            self.arch_interaction_itself = arch_interaction_itself
            self.sync_dense_params = sync_dense_params
            self.loss_threshold = loss_threshold
            self.bf16 = bf16
            self.use_ipex = use_ipex
            self.sparse_dense_boundary = sparse_dense_boundary
            # create variables for QR embedding if applicable
            self.qr_flag = qr_flag
            if self.qr_flag:
                self.qr_collisions = qr_collisions
                self.qr_operation = qr_operation
                self.qr_threshold = qr_threshold
            # create variables for MD embedding if applicable
            self.md_flag = md_flag
            if self.md_flag:
                self.md_threshold = md_threshold

            # generate np seeds for Emb table initialization
            self.l_emb_seeds = np.random.randint(low=0, high=100000, size=len(ln_emb))

            #If running distributed, get local slice of embedding tables
            if ext_dist.my_size > 1:
                n_emb = len(ln_emb)
                self.n_global_emb = n_emb
                self.rank = ext_dist.dist.get_rank()
                self.ln_emb_dense = [i for i in range(n_emb) if ln_emb[i] < self.sparse_dense_boundary]
                self.ln_emb_sparse = [i for i in range(n_emb) if ln_emb[i] >= self.sparse_dense_boundary]
                n_emb_sparse = len(self.ln_emb_sparse)
                self.n_local_emb_sparse, self.n_sparse_emb_per_rank = ext_dist.get_split_lengths(n_emb_sparse)
                self.local_ln_emb_sparse_slice = ext_dist.get_my_slice(n_emb_sparse)
                self.local_ln_emb_sparse = self.ln_emb_sparse[self.local_ln_emb_sparse_slice]
            # create operators
            if ndevices <= 1:
                if ext_dist.my_size > 1:
                    _, self.emb_dense, self.emb_sparse = self.create_emb(m_spa, ln_emb, self.local_ln_emb_sparse, self.ln_emb_dense)
                else:
                    self.emb_l, _, _ = self.create_emb(m_spa, ln_emb)

            self.bot_l = self.create_mlp(ln_bot, sigmoid_bot)
            self.top_l = self.create_mlp(ln_top, sigmoid_top)

    def apply_mlp(self, x, layers):
        # approach 1: use ModuleList
        # for layer in layers:
        #     x = layer(x)
        # return x
        # approach 2: use Sequential container to wrap all layers
        need_padding = self.use_ipex and self.bf16 and x.size(0) % 2 == 1
        if need_padding:
            x = torch.nn.functional.pad(input=x, pad=(0,0,0,1), mode='constant', value=0)
            ret = layers(x)
            return(ret[:-1,:])
        else:
            return layers(x)

    def apply_emb(self, lS_o, lS_i, emb_l):
        # WARNING: notice that we are processing the batch at once. We implicitly
        # assume that the data is laid out such that:
        # 1. each embedding is indexed with a group of sparse indices,
        #   corresponding to a single lookup
        # 2. for each embedding the lookups are further organized into a batch
        # 3. for a list of embedding tables there is a list of batched lookups

        ly = []
        for k, sparse_index_group_batch in enumerate(lS_i):
            sparse_offset_group_batch = lS_o[k]

            # embedding lookup
            # We are using EmbeddingBag, which implicitly uses sum operator.
            # The embeddings are represented as tall matrices, with sum
            # happening vertically across 0 axis, resulting in a row vector
            E = emb_l[k]
            V = E(sparse_index_group_batch, sparse_offset_group_batch)

            ly.append(V)

        # print(ly)
        return ly

    def interact_features(self, x, ly):
        x = x.to(ly[0].dtype)
        if self.arch_interaction_op == "dot":
            if self.bf16:
                T = [x] + ly
                R = ipex.interaction(*T)
            else:
                # concatenate dense and sparse features
                (batch_size, d) = x.shape
                T = torch.cat([x] + ly, dim=1).view((batch_size, -1, d))
                # perform a dot product
                Z = torch.bmm(T, torch.transpose(T, 1, 2))
                # append dense feature with the interactions (into a row vector)
                # approach 1: all
                # Zflat = Z.view((batch_size, -1))
                # approach 2: unique
                _, ni, nj = Z.shape
                # approach 1: tril_indices
                # offset = 0 if self.arch_interaction_itself else -1
                # li, lj = torch.tril_indices(ni, nj, offset=offset)
                # approach 2: custom
                offset = 1 if self.arch_interaction_itself else 0
                li = torch.tensor([i for i in range(ni) for j in range(i + offset)])
                lj = torch.tensor([j for i in range(nj) for j in range(i + offset)])
                Zflat = Z[:, li, lj]
                # concatenate dense features and interactions
                R = torch.cat([x] + [Zflat], dim=1)
        elif self.arch_interaction_op == "cat":
            # concatenation features (into a row vector)
            R = torch.cat([x] + ly, dim=1)
        else:
            sys.exit(
                "ERROR: --arch-interaction-op="
                + self.arch_interaction_op
                + " is not supported"
            )

        return R

    def forward(self, dense_x, lS_o, lS_i):
        if self.bf16:
            dense_x = dense_x.bfloat16()
        if ext_dist.my_size > 1:
            return self.distributed_forward(dense_x, lS_o, lS_i)
        elif self.ndevices <= 1:
            return self.sequential_forward(dense_x, lS_o, lS_i)
        else:
            return self.parallel_forward(dense_x, lS_o, lS_i)

    def sequential_forward(self, dense_x, lS_o, lS_i):
        # process dense features (using bottom mlp), resulting in a row vector
        x = self.apply_mlp(dense_x, self.bot_l)
        # debug prints
        # print("intermediate")
        # print(x.detach().cpu().numpy())

        # process sparse features(using embeddings), resulting in a list of row vectors
        ly = self.apply_emb(lS_o, lS_i, self.emb_l)
        # for y in ly:
        #     print(y.detach().cpu().numpy())

        # interact features (dense and sparse)
        z = self.interact_features(x, ly)
        # print(z.detach().cpu().numpy())

        # obtain probability of a click (using top mlp)
        p = self.apply_mlp(z, self.top_l)

        # clamp output if needed
        if 0.0 < self.loss_threshold and self.loss_threshold < 1.0:
            z = torch.clamp(p, min=self.loss_threshold, max=(1.0 - self.loss_threshold))
        else:
            z = p

        return z

    def distributed_forward(self, dense_x, lS_o, lS_i):
        batch_size = dense_x.size()[0]
        # WARNING: # of ranks must be <= batch size in distributed_forward call
        if batch_size < ext_dist.my_size:
            sys.exit("ERROR: batch_size (%d) must be larger than number of ranks (%d)" % (batch_size, ext_dist.my_size))

        lS_o_dense = [lS_o[i]  for i in self.ln_emb_dense]
        lS_i_dense = [lS_i[i] for i in self.ln_emb_dense]
        lS_o_sparse = [lS_o[i] for i in self.ln_emb_sparse]  # partition sparse table in one group
        lS_i_sparse = [lS_i[i] for i in self.ln_emb_sparse]

        lS_i_sparse = ext_dist.shuffle_data(lS_i_sparse)
        g_i_sparse = [lS_i_sparse[:, i * batch_size:(i + 1) * batch_size].reshape(-1) for i in range(len(self.local_ln_emb_sparse))]
        offset = torch.arange(batch_size * ext_dist.my_size).to(device)
        g_o_sparse = [offset for i in range(self.n_local_emb_sparse)]

        if (len(self.local_ln_emb_sparse) != len(g_o_sparse)) or (len(self.local_ln_emb_sparse) != len(g_i_sparse)):
            sys.exit("ERROR 0 : corrupted model input detected in distributed_forward call")
        # sparse embeddings
        ly_sparse = self.apply_emb(g_o_sparse, g_i_sparse, self.emb_sparse)
        a2a_req = ext_dist.alltoall(ly_sparse, self.n_sparse_emb_per_rank)
        # bottom mlp
        x = self.apply_mlp(dense_x, self.bot_l)
        # dense embeddings
        ly_dense = self.apply_emb(lS_o_dense, lS_i_dense, self.emb_dense)
        ly_sparse = a2a_req.wait()
        ly = ly_dense + list(ly_sparse)
        # interactions
        z = self.interact_features(x, ly)
        # top mlp
        p = self.apply_mlp(z, self.top_l)
        # clamp output if needed
        if 0.0 < self.loss_threshold and self.loss_threshold < 1.0:
            z = torch.clamp(
                p, min=self.loss_threshold, max=(1.0 - self.loss_threshold)
            )
        else:
            z = p

        return z

    def parallel_forward(self, dense_x, lS_o, lS_i):
        ### prepare model (overwrite) ###
        # WARNING: # of devices must be >= batch size in parallel_forward call
        batch_size = dense_x.size()[0]
        ndevices = min(self.ndevices, batch_size, len(self.emb_l))
        device_ids = range(ndevices)
        # WARNING: must redistribute the model if mini-batch size changes(this is common
        # for last mini-batch, when # of elements in the dataset/batch size is not even
        if self.parallel_model_batch_size != batch_size:
            self.parallel_model_is_not_prepared = True

        if self.parallel_model_is_not_prepared or self.sync_dense_params:
            # replicate mlp (data parallelism)
            self.bot_l_replicas = replicate(self.bot_l, device_ids)
            self.top_l_replicas = replicate(self.top_l, device_ids)
            self.parallel_model_batch_size = batch_size

        if self.parallel_model_is_not_prepared:
            # distribute embeddings (model parallelism)
            t_list = []
            for k, emb in enumerate(self.emb_l):
                d = torch.device("cuda:" + str(k % ndevices))
                emb.to(d)
                t_list.append(emb.to(d))
            self.emb_l = nn.ModuleList(t_list)
            self.parallel_model_is_not_prepared = False

        ### prepare input (overwrite) ###
        # scatter dense features (data parallelism)
        # print(dense_x.device)
        dense_x = scatter(dense_x, device_ids, dim=0)
        # distribute sparse features (model parallelism)
        if (len(self.emb_l) != len(lS_o)) or (len(self.emb_l) != len(lS_i)):
            sys.exit("ERROR: corrupted model input detected in parallel_forward call")

        t_list = []
        i_list = []
        for k, _ in enumerate(self.emb_l):
            d = torch.device("cuda:" + str(k % ndevices))
            t_list.append(lS_o[k].to(d))
            i_list.append(lS_i[k].to(d))
        lS_o = t_list
        lS_i = i_list

        ### compute results in parallel ###
        # bottom mlp
        # WARNING: Note that the self.bot_l is a list of bottom mlp modules
        # that have been replicated across devices, while dense_x is a tuple of dense
        # inputs that has been scattered across devices on the first (batch) dimension.
        # The output is a list of tensors scattered across devices according to the
        # distribution of dense_x.
        x = parallel_apply(self.bot_l_replicas, dense_x, None, device_ids)
        # debug prints
        # print(x)

        # embeddings
        ly = self.apply_emb(lS_o, lS_i, self.emb_l)
        # debug prints
        # print(ly)

        # butterfly shuffle (implemented inefficiently for now)
        # WARNING: Note that at this point we have the result of the embedding lookup
        # for the entire batch on each device. We would like to obtain partial results
        # corresponding to all embedding lookups, but part of the batch on each device.
        # Therefore, matching the distribution of output of bottom mlp, so that both
        # could be used for subsequent interactions on each device.
        if len(self.emb_l) != len(ly):
            sys.exit("ERROR: corrupted intermediate result in parallel_forward call")

        t_list = []
        for k, _ in enumerate(self.emb_l):
            d = torch.device("cuda:" + str(k % ndevices))
            y = scatter(ly[k], device_ids, dim=0)
            t_list.append(y)
        # adjust the list to be ordered per device
        ly = list(map(lambda y: list(y), zip(*t_list)))
        # debug prints
        # print(ly)

        # interactions
        z = []
        for k in range(ndevices):
            zk = self.interact_features(x[k], ly[k])
            z.append(zk)
        # debug prints
        # print(z)

        # top mlp
        # WARNING: Note that the self.top_l is a list of top mlp modules that
        # have been replicated across devices, while z is a list of interaction results
        # that by construction are scattered across devices on the first (batch) dim.
        # The output is a list of tensors scattered across devices according to the
        # distribution of z.
        p = parallel_apply(self.top_l_replicas, z, None, device_ids)

        ### gather the distributed results ###
        p0 = gather(p, self.output_d, dim=0)

        # clamp output if needed
        if 0.0 < self.loss_threshold and self.loss_threshold < 1.0:
            z0 = torch.clamp(
                p0, min=self.loss_threshold, max=(1.0 - self.loss_threshold)
            )
        else:
            z0 = p0

        return z0


class ModelArguments:
    def __init__(self,
                 dist_backend: str = "",
                 backend_url: str = "",
                 sparse_dense_boundary: int = 2048,
                 bf16: bool = False,
                 use_ipex: bool = False,
                 arch_dense_feature_size: int = 2,
                 arch_sparse_feature_size: int = 2,
                 arch_embedding_size: str = "4-3-2",
                 arch_mlp_bot: str = "4-3-2",
                 arch_mlp_top: str = "4-3-2",
                 arch_interaction_op: str = "dot",
                 arch_interaction_itself: bool = False,
                 md_flag: False = False,
                 md_threshold: int = 200,
                 md_temperature: float = 0.3,
                 md_round_dims: bool = False,
                 qr_flag: bool = False,
                 qr_threshold: int = 200,
                 qr_operation: str = "mult",
                 qr_collisions: int = 4,
                 activation_function: str = "relu",
                 loss_function: str = "mse",
                 loss_weights: str = "1.0-1.0",
                 loss_threshold: float = 0.0,
                 round_targets: bool = False,
                 data_size: int = 1,
                 num_batches: int = 0,
                 data_generation: str = "random",
                 data_trace_file: str = "./input/dist_emb_j.log",
                 data_set: str = "kaggle",
                 raw_data_file: str = "",
                 processed_data_file: str = "",
                 data_randomize: str = "total",
                 data_trace_enable_padding: bool = False,
                 max_ind_range: int = -1,
                 data_sub_saple_rate: float = 0.0,
                 num_indices_per_lookup: int = 10,
                 num_indices_per_lookup_fixed: bool = False,
                 num_workers: int = 0,
                 memory_map: bool = False,
                 mini_batch_size: int = 1,
                 nepochs: int = 1,
                 learning_rate: float = 0.01,
                 print_precision: int = 5,
                 numpy_rand_seed: int = 123,
                 sync_dense_params: bool = True,
                 inference_only: bool = False,
                 save_onnx: bool = False,
                 use_gpu: bool = False,
                 print_freq: int = 1,
                 test_freq: int = -1,
                 test_mini_batch_size: int = -1,
                 test_num_workers: int = -1,
                 print_time: bool = False,
                 debug_mode: bool = False,
                 enable_profiling: bool = False,
                 plot_compute_graph: bool = False,
                 save_model: str = "",
                 load_model: str = "",
                 mlperf_logging: bool = False,
                 mlperf_acc_threshold: float = 0.0,
                 mlperf_auc_threshold: float = 0.0,
                 mlperf_bin_loader: bool = False,
                 mlperf_bin_shuffle: bool = False,
                 lr_num_warmup_steps: int = 0,
                 lr_decay_start_step: int = 0,
                 lr_num_decay_steps: int = 0):

        self.dist_backend = dist_backend
        self.backend_url = backend_url
        self.sparse_dense_boundary = sparse_dense_boundary
        self.bf16 = bf16
        self.use_ipex = use_ipex

        self.arch_dense_feature_size: int = arch_dense_feature_size
        self.arch_sparse_feature_size: int = arch_sparse_feature_size
        self.arch_embedding_size: str = arch_embedding_size
        self.arch_mlp_bot: str = arch_mlp_bot
        self.arch_mlp_top: str = arch_mlp_top
        self.arch_interaction_op: str = arch_interaction_op
        self.arch_interaction_itself: bool = arch_interaction_itself
        self.md_flag: False = md_flag
        self.md_threshold: int =md_threshold
        self.md_temperature: float = md_temperature
        self.md_round_dims: bool = md_round_dims
        self.qr_flag: bool = qr_flag
        self.qr_threshold: int = qr_threshold
        self.qr_operation: str = qr_operation
        self.qr_collisions: int = qr_collisions
        self.activation_function: str = activation_function
        self.loss_function: str = loss_function
        self.loss_weights: str = loss_weights
        self.loss_threshold: float = loss_threshold
        self.round_targets: bool = round_targets
        self.data_size: int = data_size
        self.num_batches: int = num_batches
        self.data_generation: str = data_generation
        self.data_trace_file: str = data_trace_file
        self.data_set: str = data_set
        self.raw_data_file: str = raw_data_file
        self.processed_data_file: str = processed_data_file
        self.data_randomize: str = data_randomize
        self.data_trace_enable_padding: bool = data_trace_enable_padding
        self.max_ind_range: int = max_ind_range
        self.data_sub_saple_rate: float = data_sub_saple_rate
        self.num_indices_per_lookup: int = num_indices_per_lookup
        self.num_indices_per_lookup_fixed: bool = num_indices_per_lookup_fixed
        self.num_workers: int =num_workers
        self.memory_map: bool = memory_map
        self.mini_batch_size: int = mini_batch_size
        self.nepochs: int = nepochs
        self.learning_rate: float = learning_rate
        self.print_precision: int = print_precision
        self.numpy_rand_seed: int = numpy_rand_seed
        self.sync_dense_params: bool = sync_dense_params
        self.inference_only: bool = inference_only
        self.save_onnx: bool = save_onnx
        self.use_gpu: bool = use_gpu
        self.print_freq: int = print_freq
        self.test_freq: int = test_freq
        self.test_mini_batch_size: int = test_mini_batch_size
        self.test_num_workers: int = test_num_workers
        self.print_time: bool = print_time
        self.debug_mode: bool = debug_mode
        self.enable_profiling: bool = enable_profiling
        self.plot_compute_graph: bool = plot_compute_graph
        self.save_model: str = save_model
        self.load_model: str = load_model
        self.mlperf_logging: bool = mlperf_logging
        self.mlperf_acc_threshold: float = mlperf_acc_threshold
        self.mlperf_auc_threshold: float = mlperf_auc_threshold
        self.mlperf_bin_loader: bool = mlperf_bin_loader
        self.mlperf_bin_shuffle: bool = mlperf_bin_shuffle
        self.lr_num_warmup_steps: int = lr_num_warmup_steps
        self.lr_decay_start_step: int = lr_decay_start_step
        self.lr_num_decay_steps: int = lr_num_decay_steps


def model_training(args: ModelArguments,
                   train_ld,
                   test_ld,
                   model_size,
                   nbatches=-1,
                   nbatches_test=-1):
    start_time = time.time()
    # the reference implementation doesn't clear the cache currently
    # but the submissions are required to do that
    mlperf_logger.log_event(key=mlperf_logger.constants.CACHE_CLEAR, value=True)

    mlperf_logger.log_start(key=mlperf_logger.constants.INIT_START, log_all_ranks=True)

    ### import packages ###
    import sys

    ext_dist.init_distributed(backend=args.dist_backend)

    if args.mlperf_logging:
        print('command line args: ', json.dumps(vars(args)))

    ### some basic setup ###
    np.random.seed(args.numpy_rand_seed)
    np.set_printoptions(precision=args.print_precision)
    torch.set_printoptions(precision=args.print_precision)
    torch.manual_seed(args.numpy_rand_seed)

    if (args.test_mini_batch_size < 0):
        # if the parameter is not set, use the training batch size
        args.test_mini_batch_size = args.mini_batch_size
    if (args.test_num_workers < 0):
        # if the parameter is not set, use the same parameter for training
        args.test_num_workers = args.num_workers
    if (args.mini_batch_size % ext_dist.my_size !=0 or args.test_mini_batch_size % ext_dist.my_size != 0):
        print("Either test minibatch (%d) or train minibatch (%d) does not split across %d ranks" % (args.test_mini_batch_size, args.mini_batch_size, ext_dist.my_size))
        sys.exit(1)

    use_gpu = args.use_gpu and torch.cuda.is_available()
    use_ipex = args.use_ipex
    global device
    if use_gpu:
        torch.cuda.manual_seed_all(args.numpy_rand_seed)
        torch.backends.cudnn.deterministic = True
        if ext_dist.my_size > 1:
            ngpus = torch.cuda.device_count()  # 1
            if ext_dist.my_local_size > torch.cuda.device_count():
                print("Not sufficient GPUs available... local_size = %d, ngpus = %d" % (ext_dist.my_local_size, ngpus))
                sys.exit(1)
            ngpus = 1
            device = torch.device("cuda", ext_dist.my_local_rank)
        else:
            device = torch.device("cuda", 0)
            ngpus = torch.cuda.device_count()  # 1
        print("Using {} GPU(s)...".format(ngpus))
    elif use_ipex:
        device = torch.device("dpcpp")
        print("Using IPEX...")
    else:
        device = torch.device("cpu")
        print("Using CPU...")

    ### prepare training data ###
    ln_bot = np.fromstring(args.arch_mlp_bot, dtype=int, sep="-")
    # input data

    mlperf_logger.barrier()
    mlperf_logger.log_end(key=mlperf_logger.constants.INIT_STOP)
    mlperf_logger.barrier()
    mlperf_logger.log_start(key=mlperf_logger.constants.RUN_START)
    mlperf_logger.barrier()

    ### prepare data set
    ln_emb = list(model_size.values())
    # enforce maximum limit on number of vectors per embedding
    if args.max_ind_range > 0:
        ln_emb = np.array(list(map(
            lambda x: x if x < args.max_ind_range else args.max_ind_range, ln_emb)))

    m_den = args.arch_dense_feature_size
    ln_bot[0] = m_den
    if nbatches <= 0:
        nbatches = len(train_ld) // (args.mini_batch_size // ext_dist.my_size)
    if nbatches_test <= 0:
        nbatches_test = len(test_ld) // (args.test_mini_batch_size // ext_dist.my_size)

    # if (args.data_generation == "dataset"):
    #     train_data, train_ld, test_data, test_ld = \
    #         dp.make_criteo_data_and_loaders(args)
    #     nbatches = args.num_batches if args.num_batches > 0 else len(train_ld)
    #     nbatches_test = len(test_ld)
    #
    #     ln_emb = train_data.counts
    #     # enforce maximum limit on number of vectors per embedding
    #     if args.max_ind_range > 0:
    #         ln_emb = np.array(list(map(
    #             lambda x: x if x < args.max_ind_range else args.max_ind_range,
    #             ln_emb
    #         )))
    #     m_den = train_data.m_den
    #     ln_bot[0] = m_den
    # else:
    #     # input and target at random
    #     ln_emb = np.fromstring(args.arch_embedding_size, dtype=int, sep="-")
    #     m_den = ln_bot[0]
    #     train_data, train_ld = dp.make_random_data_and_loader(args, ln_emb, m_den)
    #     nbatches = args.num_batches if args.num_batches > 0 else len(train_ld)

    ### parse command line arguments ###
    m_spa = args.arch_sparse_feature_size
    num_fea = ln_emb.size + 1  # num sparse + num dense features
    m_den_out = ln_bot[ln_bot.size - 1]
    if args.arch_interaction_op == "dot":
        # approach 1: all
        # num_int = num_fea * num_fea + m_den_out
        # approach 2: unique
        if args.arch_interaction_itself:
            num_int = (num_fea * (num_fea + 1)) // 2 + m_den_out
        else:
            num_int = (num_fea * (num_fea - 1)) // 2 + m_den_out
    elif args.arch_interaction_op == "cat":
        num_int = num_fea * m_den_out
    else:
        sys.exit(
            "ERROR: --arch-interaction-op="
            + args.arch_interaction_op
            + " is not supported"
        )
    arch_mlp_top_adjusted = str(num_int) + "-" + args.arch_mlp_top
    ln_top = np.fromstring(arch_mlp_top_adjusted, dtype=int, sep="-")

    # sanity check: feature sizes and mlp dimensions must match
    if m_den != ln_bot[0]:
        sys.exit(
            "ERROR: arch-dense-feature-size "
            + str(m_den)
            + " does not match first dim of bottom mlp "
            + str(ln_bot[0])
        )
    if args.qr_flag:
        if args.qr_operation == "concat" and 2 * m_spa != m_den_out:
            sys.exit(
                "ERROR: 2 arch-sparse-feature-size "
                + str(2 * m_spa)
                + " does not match last dim of bottom mlp "
                + str(m_den_out)
                + " (note that the last dim of bottom mlp must be 2x the embedding dim)"
            )
        if args.qr_operation != "concat" and m_spa != m_den_out:
            sys.exit(
                "ERROR: arch-sparse-feature-size "
                + str(m_spa)
                + " does not match last dim of bottom mlp "
                + str(m_den_out)
            )
    else:
        if m_spa != m_den_out:
            sys.exit(
                "ERROR: arch-sparse-feature-size "
                + str(m_spa)
                + " does not match last dim of bottom mlp "
                + str(m_den_out)
            )
    if num_int != ln_top[0]:
        sys.exit(
            "ERROR: # of feature interactions "
            + str(num_int)
            + " does not match first dimension of top mlp "
            + str(ln_top[0])
        )

    # assign mixed dimensions if applicable
    if args.md_flag:
        m_spa = md_solver(
            torch.tensor(ln_emb),
            args.md_temperature,  # alpha
            d0=m_spa,
            round_dim=args.md_round_dims
        ).tolist()

    # test prints (model arch)
    if args.debug_mode:
        print("model arch:")
        print(
            "mlp top arch "
            + str(ln_top.size - 1)
            + " layers, with input to output dimensions:"
        )
        print(ln_top)
        print("# of interactions")
        print(num_int)
        print(
            "mlp bot arch "
            + str(ln_bot.size - 1)
            + " layers, with input to output dimensions:"
        )
        print(ln_bot)
        print("# of features (sparse and dense)")
        print(num_fea)
        print("dense feature size")
        print(m_den)
        print("sparse feature size")
        print(m_spa)
        print(
            "# of embeddings (= # of sparse features) "
            + str(ln_emb.size)
            + ", with dimensions "
            + str(m_spa)
            + "x:"
        )
        print(ln_emb)

        print("data (inputs and targets):")
        for j, (X, lS_o, lS_i, T) in enumerate(train_ld):
            # early exit if nbatches was set by the user and has been exceeded
            if nbatches > 0 and j >= nbatches:
                break

            print("mini-batch: %d" % j)
            print(X.detach().cpu().numpy())
            # transform offsets to lengths when printing
            print(
                [
                    np.diff(
                        S_o.detach().cpu().tolist() + list(lS_i[i].shape)
                    ).tolist()
                    for i, S_o in enumerate(lS_o)
                ]
            )
            print([S_i.detach().cpu().tolist() for S_i in lS_i])
            print(T.detach().cpu().numpy())

    ndevices = min(ngpus, args.mini_batch_size, num_fea - 1) if use_gpu else -1

    ### construct the neural network specified above ###
    # WARNING: to obtain exactly the same initialization for
    # the weights we need to start from the same random seed.
    # np.random.seed(args.numpy_rand_seed)
    print('Creating the model...')
    dlrm = DLRM_Net(
        m_spa,
        ln_emb,
        ln_bot,
        ln_top,
        arch_interaction_op=args.arch_interaction_op,
        arch_interaction_itself=args.arch_interaction_itself,
        sigmoid_bot=-1,
        sigmoid_top=ln_top.size - 2,
        sync_dense_params=args.sync_dense_params,
        loss_threshold=args.loss_threshold,
        ndevices=ndevices,
        qr_flag=args.qr_flag,
        qr_operation=args.qr_operation,
        qr_collisions=args.qr_collisions,
        qr_threshold=args.qr_threshold,
        md_flag=args.md_flag,
        md_threshold=args.md_threshold,
        sparse_dense_boundary=args.sparse_dense_boundary,
        bf16 = args.bf16,
        use_ipex = args.use_ipex
    )

    print('Model created!')
    # test prints
    if args.debug_mode:
        print("initial parameters (weights and bias):")
        for param in dlrm.parameters():
            print(param.detach().cpu().numpy())
        # print(dlrm)

    if args.use_ipex:
        dlrm = dlrm.to(device)
        print(dlrm, device, args.use_ipex)

    if use_gpu:
        # Custom Model-Data Parallel
        # the mlps are replicated and use data parallelism, while
        # the embeddings are distributed and use model parallelism
        dlrm = dlrm.to(device)  # .cuda()
        if dlrm.ndevices > 1:
            dlrm.emb_l = dlrm.create_emb(m_spa, ln_emb)

    if ext_dist.my_size > 1:
        if use_gpu:
            device_ids = [ext_dist.my_local_rank]
            dlrm.bot_l = ext_dist.DDP(dlrm.bot_l, device_ids=device_ids)
            dlrm.top_l = ext_dist.DDP(dlrm.top_l, device_ids=device_ids)
        else:
            dlrm.bot_l = ext_dist.DDP(dlrm.bot_l)
            dlrm.top_l = ext_dist.DDP(dlrm.top_l)
            for i in range(len(dlrm.emb_dense)):
                dlrm.emb_dense[i] = ext_dist.DDP(dlrm.emb_dense[i])

    # specify the loss function
    if args.loss_function == "mse":
        loss_fn = torch.nn.MSELoss(reduction="mean")
    elif args.loss_function == "bce":
        loss_fn = torch.nn.BCELoss(reduction="mean")
    elif args.loss_function == "wbce":
        loss_ws = torch.tensor(np.fromstring(args.loss_weights, dtype=float, sep="-"))
        loss_fn = torch.nn.BCELoss(reduction="none")
    else:
        sys.exit("ERROR: --loss-function=" + args.loss_function + " is not supported")

    if not args.inference_only:
        # specify the optimizer algorithm
        if ext_dist.my_size == 1:
            if args.bf16 and ipex.is_available():
                optimizer = ipex.SplitSGD(dlrm.parameters(), lr=args.learning_rate)
            else:
                optimizer = torch.optim.SGD(dlrm.parameters(), lr=args.learning_rate)
        else:
            if args.bf16 and ipex.is_available():
                optimizer = ipex.SplitSGD([
                    {"params": [p for emb in dlrm.emb_sparse for p in emb.parameters()], "lr" : args.learning_rate / ext_dist.my_size},
                    {"params": [p for emb in dlrm.emb_dense for p in emb.parameters()], "lr" : args.learning_rate},
                    {"params": dlrm.bot_l.parameters(), "lr" : args.learning_rate},
                    {"params": dlrm.top_l.parameters(), "lr" : args.learning_rate}
                ], lr=args.learning_rate)
            else:
                optimizer = torch.optim.SGD([
                    {"params": [p for emb in dlrm.emb_sparse for p in emb.parameters()], "lr" : args.learning_rate / ext_dist.my_size},
                    {"params": [p for emb in dlrm.emb_dense for p in emb.parameters()], "lr" : args.learning_rate},
                    {"params": dlrm.bot_l.parameters(), "lr" : args.learning_rate},
                    {"params": dlrm.top_l.parameters(), "lr" : args.learning_rate}
                ], lr=args.learning_rate)

        lr_scheduler = LRPolicyScheduler(optimizer, args.lr_num_warmup_steps, args.lr_decay_start_step,
                                         args.lr_num_decay_steps)

    ### main loop ###
    def time_wrap(use_gpu):
        if use_gpu:
            torch.cuda.synchronize()
        return time.time()

    def dlrm_wrap(X, lS_o, lS_i, use_gpu, use_ipex, device):
        if use_gpu or use_ipex:  # .cuda()
            # lS_i can be either a list of tensors or a stacked tensor.
            # Handle each case below:
            lS_i = [S_i.to(device) for S_i in lS_i] if isinstance(lS_i, list) \
                else lS_i.to(device)
            lS_o = [S_o.to(device) for S_o in lS_o] if isinstance(lS_o, list) \
                else lS_o.to(device)
            return dlrm(
                X.to(device),
                lS_o,
                lS_i
            )
        else:
            return dlrm(X, lS_o, lS_i)

    def loss_fn_wrap(Z, T, use_gpu, use_ipex, device):
        if args.loss_function == "mse" or args.loss_function == "bce":
            if use_gpu or use_ipex:
                return loss_fn(Z, T.to(device))
            else:
                return loss_fn(Z, T)
        elif args.loss_function == "wbce":
            if use_gpu:
                loss_ws_ = loss_ws[T.data.view(-1).long()].view_as(T).to(device)
                loss_fn_ = loss_fn(Z, T.to(device))
            else:
                loss_ws_ = loss_ws[T.data.view(-1).long()].view_as(T)
                loss_fn_ = loss_fn(Z, T.to(device))
            loss_sc_ = loss_ws_ * loss_fn_
            # debug prints
            # print(loss_ws_)
            # print(loss_fn_)
            return loss_sc_.mean()

    # training or inference
    best_gA_test = 0
    best_auc_test = 0
    skip_upto_epoch = 0
    skip_upto_batch = 0
    total_time = 0
    total_loss = 0
    total_accu = 0
    total_iter = 0
    total_samp = 0
    k = 0

    mlperf_logger.mlperf_submission_log('dlrm')
    mlperf_logger.log_event(key=mlperf_logger.constants.SEED, value=args.numpy_rand_seed)
    mlperf_logger.log_event(key=mlperf_logger.constants.GLOBAL_BATCH_SIZE, value=args.mini_batch_size)

    # Load model is specified
    if not (args.load_model == ""):
        print("Loading saved model {}".format(args.load_model))
        if use_gpu:
            if dlrm.ndevices > 1:
                # NOTE: when targeting inference on multiple GPUs,
                # load the model as is on CPU or GPU, with the move
                # to multiple GPUs to be done in parallel_forward
                ld_model = torch.load(args.load_model)
            else:
                # NOTE: when targeting inference on single GPU,
                # note that the call to .to(device) has already happened
                ld_model = torch.load(
                    args.load_model,
                    map_location=torch.device('cuda')
                    # map_location=lambda storage, loc: storage.cuda(0)
                )
        else:
            # when targeting inference on CPU
            ld_model = torch.load(args.load_model, map_location=torch.device('cpu'))
        dlrm.load_state_dict(ld_model["state_dict"])
        ld_j = ld_model["iter"]
        ld_k = ld_model["epoch"]
        ld_nepochs = ld_model["nepochs"]
        ld_nbatches = ld_model["nbatches"]
        ld_nbatches_test = ld_model["nbatches_test"]
        ld_gA = ld_model["train_acc"]
        ld_gL = ld_model["train_loss"]
        ld_total_loss = ld_model["total_loss"]
        ld_total_accu = ld_model["total_accu"]
        ld_gA_test = ld_model["test_acc"]
        ld_gL_test = ld_model["test_loss"]
        if not args.inference_only:
            optimizer.load_state_dict(ld_model["opt_state_dict"])
            best_gA_test = ld_gA_test
            total_loss = ld_total_loss
            total_accu = ld_total_accu
            skip_upto_epoch = ld_k  # epochs
            skip_upto_batch = ld_j  # batches
        else:
            args.print_freq = ld_nbatches
            args.test_freq = 0

        print(
            "Saved at: epoch = {:d}/{:d}, batch = {:d}/{:d}, ntbatch = {:d}".format(
                ld_k, ld_nepochs, ld_j, ld_nbatches, ld_nbatches_test
            )
        )
        print(
            "Training state: loss = {:.6f}, accuracy = {:3.3f} %".format(
                ld_gL, ld_gA * 100
            )
        )
        print(
            "Testing state: loss = {:.6f}, accuracy = {:3.3f} %".format(
                ld_gL_test, ld_gA_test * 100
            )
        )

    ext_dist.barrier()
    print("time/loss/accuracy (if enabled):")

    # LR is logged twice for now because of a compliance checker bug
    mlperf_logger.log_event(key=mlperf_logger.constants.OPT_BASE_LR, value=args.learning_rate)
    mlperf_logger.log_event(key=mlperf_logger.constants.OPT_LR_WARMUP_STEPS,
                            value=args.lr_num_warmup_steps)

    # use logging keys from the official HP table and not from the logging library
    mlperf_logger.log_event(key='sgd_opt_base_learning_rate', value=args.learning_rate)
    mlperf_logger.log_event(key='lr_decay_start_steps', value=args.lr_decay_start_step)
    mlperf_logger.log_event(key='sgd_opt_learning_rate_decay_steps', value=args.lr_num_decay_steps)
    mlperf_logger.log_event(key='sgd_opt_learning_rate_decay_poly_power', value=2)

    with torch.autograd.profiler.profile(args.enable_profiling, use_gpu) as prof:
        while k < args.nepochs:
            mlperf_logger.barrier()
            mlperf_logger.log_start(key=mlperf_logger.constants.BLOCK_START,
                                    metadata={mlperf_logger.constants.FIRST_EPOCH_NUM: (k + 1),
                                              mlperf_logger.constants.EPOCH_COUNT: 1})
            mlperf_logger.barrier()
            mlperf_logger.log_start(key=mlperf_logger.constants.EPOCH_START,
                                    metadata={mlperf_logger.constants.EPOCH_NUM: k + 1})

            if k < skip_upto_epoch:
                continue

            accum_time_begin = time_wrap(use_gpu)

            if args.mlperf_logging:
                previous_iteration_time = None

            for j, (X, lS_o, lS_i, T) in enumerate(train_ld):
                if j == 0 and args.save_onnx:
                    (X_onnx, lS_o_onnx, lS_i_onnx) = (X, lS_o, lS_i)

                if j < skip_upto_batch:
                    continue

                if args.mlperf_logging:
                    current_time = time_wrap(use_gpu)
                    if previous_iteration_time:
                        iteration_time = current_time - previous_iteration_time
                    else:
                        iteration_time = 0
                    previous_iteration_time = current_time
                else:
                    ext_dist.barrier()
                    t1 = time_wrap(use_gpu)

                # early exit if nbatches was set by the user and has been exceeded
                if nbatches > 0 and j >= nbatches:
                    break
                '''
                # debug prints
                print("input and targets")
                print(X.detach().cpu().numpy())
                print([np.diff(S_o.detach().cpu().tolist()
                       + list(lS_i[i].shape)).tolist() for i, S_o in enumerate(lS_o)])
                print([S_i.detach().cpu().numpy().tolist() for S_i in lS_i])
                print(T.detach().cpu().numpy())
                '''

                # forward pass
                Z = dlrm_wrap(X, lS_o, lS_i, use_gpu, use_ipex, device)

                # loss
                E = loss_fn_wrap(Z, T, use_gpu, use_ipex, device)
                '''
                # debug prints
                print("output and loss")
                print(Z.detach().cpu().numpy())
                print(E.detach().cpu().numpy())
                '''
                #print(E.detach().cpu().numpy(), flush=True)
                # compute loss and accuracy
                L = E.detach().cpu().numpy()  # numpy array
                S = Z.detach().cpu().numpy()  # numpy array
                T = T.detach().cpu().numpy()  # numpy array
                mbs = T.shape[0]  # = args.mini_batch_size except maybe for last
                A = np.sum((np.round(S, 0) == T).astype(np.uint8))

                if not args.inference_only:
                    # scaled error gradient propagation
                    # (where we do not accumulate gradients across mini-batches)
                    optimizer.zero_grad()
                    # backward pass
                    E.backward()
                    # debug prints (check gradient norm)
                    # for l in mlp.layers:
                    #     if hasattr(l, 'weight'):
                    #          print(l.weight.grad.norm().item())

                    # optimizer
                    optimizer.step()
                    lr_scheduler.step()

                if args.mlperf_logging:
                    total_time += iteration_time
                else:
                    t2 = time_wrap(use_gpu)
                    total_time += t2 - t1
                total_accu += A
                total_loss += L * mbs
                total_iter += 1
                total_samp += mbs

                should_print = ((j + 1) % args.print_freq == 0) or (j + 1 == nbatches)
                should_test = (
                        (args.test_freq > 0)
                        and (args.data_generation == "dataset")
                        and (((j + 1) % args.test_freq == 0) or (j + 1 == nbatches))
                )

                # print time, loss and accuracy
                if should_print or should_test:
                    gT = 1000.0 * total_time / total_iter if args.print_time else -1
                    total_time = 0

                    gA = total_accu / total_samp
                    total_accu = 0

                    gL = total_loss / total_samp
                    total_loss = 0

                    str_run_type = "inference" if args.inference_only else "training"
                    print(
                        "Finished {} it {}/{} of epoch {}, {:.2f} ms/it, ".format(
                            str_run_type, j + 1, nbatches, k, gT
                        )
                        + "loss {:.6f}, accuracy {:3.3f} %".format(gL, gA * 100)
                        , flush=True)
                    # Uncomment the line below to print out the total time with overhead
                    # print("Accumulated time so far: {}" \
                    # .format(time_wrap(use_gpu) - accum_time_begin))
                    total_iter = 0
                    total_samp = 0

                # testing
                if should_test and not args.inference_only:
                    epoch_num_float = -1 #(j + 1) / len(train_ld) + k + 1
                    mlperf_logger.barrier()
                    mlperf_logger.log_start(key=mlperf_logger.constants.EVAL_START,
                                            metadata={mlperf_logger.constants.EPOCH_NUM: epoch_num_float})

                    # don't measure training iter time in a test iteration
                    if args.mlperf_logging:
                        previous_iteration_time = None

                    test_accu = 0
                    test_loss = 0
                    test_samp = 0

                    accum_test_time_begin = time_wrap(use_gpu)
                    if args.mlperf_logging:
                        scores = []
                        targets = []

                    for i, (X_test, lS_o_test, lS_i_test, T_test) in enumerate(test_ld):
                        # early exit if nbatches was set by the user and was exceeded
                        if nbatches > 0 and i >= nbatches:
                            break

                        t1_test = time_wrap(use_gpu)

                        # forward pass
                        Z_test = dlrm_wrap(
                            X_test, lS_o_test, lS_i_test, use_gpu, use_ipex, device
                        )
                        if args.mlperf_logging:
                            if ext_dist.my_size > 1:
                                Z_test = ext_dist.all_gather(Z_test, None)
                                T_test = ext_dist.all_gather(T_test, None)
                            S_test = Z_test.detach().cpu().numpy()  # numpy array
                            T_test = T_test.detach().cpu().numpy()  # numpy array
                            scores.append(S_test)
                            targets.append(T_test)
                        else:
                            # loss
                            E_test = loss_fn_wrap(Z_test, T_test, use_gpu, use_ipex, device)

                            # compute loss and accuracy
                            L_test = E_test.detach().cpu().numpy()  # numpy array
                            S_test = Z_test.detach().cpu().numpy()  # numpy array
                            T_test = T_test.detach().cpu().numpy()  # numpy array
                            mbs_test = T_test.shape[0]  # = mini_batch_size except last
                            A_test = np.sum((np.round(S_test, 0) == T_test).astype(np.uint8))
                            test_accu += A_test
                            test_loss += L_test * mbs_test
                            test_samp += mbs_test

                        t2_test = time_wrap(use_gpu)

                    if args.mlperf_logging:
                        scores = np.concatenate(scores, axis=0)
                        targets = np.concatenate(targets, axis=0)

                        validation_results = {}
                        if args.use_ipex:
                            validation_results['roc_auc'], validation_results['loss'], validation_results['accuracy'] = \
                                core.roc_auc_score(torch.from_numpy(targets).reshape(-1), torch.from_numpy(scores).reshape(-1))
                        else:
                            metrics = {
                                'loss' : sklearn.metrics.log_loss,
                                'recall' : lambda y_true, y_score:
                                sklearn.metrics.recall_score(
                                    y_true=y_true,
                                    y_pred=np.round(y_score)
                                ),
                                'precision' : lambda y_true, y_score:
                                sklearn.metrics.precision_score(
                                    y_true=y_true,
                                    y_pred=np.round(y_score)
                                ),
                                'f1' : lambda y_true, y_score:
                                sklearn.metrics.f1_score(
                                    y_true=y_true,
                                    y_pred=np.round(y_score)
                                ),
                                'ap' : sklearn.metrics.average_precision_score,
                                'roc_auc' : sklearn.metrics.roc_auc_score,
                                'accuracy' : lambda y_true, y_score:
                                sklearn.metrics.accuracy_score(
                                    y_true=y_true,
                                    y_pred=np.round(y_score)
                                ),
                            }

                            # print("Compute time for validation metric : ", end="")
                            # first_it = True
                            for metric_name, metric_function in metrics.items():
                                # if first_it:
                                #     first_it = False
                                # else:
                                #     print(", ", end="")
                                # metric_compute_start = time_wrap(False)
                                validation_results[metric_name] = metric_function(
                                    targets,
                                    scores
                                )
                                # metric_compute_end = time_wrap(False)
                                # met_time = metric_compute_end - metric_compute_start
                                # print("{} {:.4f}".format(metric_name, 1000 * (met_time)),
                                #      end="")

                        # print(" ms")
                        gA_test = validation_results['accuracy']
                        gL_test = validation_results['loss']
                    else:
                        gA_test = test_accu / test_samp
                        gL_test = test_loss / test_samp

                    is_best = gA_test > best_gA_test
                    if is_best:
                        best_gA_test = gA_test
                        if not (args.save_model == ""):
                            print("Saving model to {}".format(args.save_model))
                            torch.save(
                                {
                                    "epoch": k,
                                    "nepochs": args.nepochs,
                                    "nbatches": nbatches,
                                    "nbatches_test": nbatches_test,
                                    "iter": j + 1,
                                    "state_dict": dlrm.state_dict(),
                                    "train_acc": gA,
                                    "train_loss": gL,
                                    "test_acc": gA_test,
                                    "test_loss": gL_test,
                                    "total_loss": total_loss,
                                    "total_accu": total_accu,
                                    "opt_state_dict": optimizer.state_dict(),
                                },
                                args.save_model,
                            )

                    if args.mlperf_logging:
                        is_best = validation_results['roc_auc'] > best_auc_test
                        if is_best:
                            best_auc_test = validation_results['roc_auc']

                        mlperf_logger.log_event(key=mlperf_logger.constants.EVAL_ACCURACY,
                                                value=float(validation_results['roc_auc']),
                                                metadata={mlperf_logger.constants.EPOCH_NUM: epoch_num_float})
                        print(
                            "Testing at - {}/{} of epoch {},".format(j + 1, nbatches, k)
                            + " loss {:.6f},".format(
                                validation_results['loss']
                            )
                            + " auc {:.4f}, best auc {:.4f},".format(
                                validation_results['roc_auc'],
                                best_auc_test
                            )
                            + " accuracy {:3.3f} %, best accuracy {:3.3f} %".format(
                                validation_results['accuracy'] * 100,
                                best_gA_test * 100
                            )
                            , flush=True)
                    else:
                        print(
                            "Testing at - {}/{} of epoch {},".format(j + 1, nbatches, 0)
                            + " loss {:.6f}, accuracy {:3.3f} %, best {:3.3f} %".format(
                                gL_test, gA_test * 100, best_gA_test * 100
                            )
                        )
                    mlperf_logger.barrier()
                    mlperf_logger.log_end(key=mlperf_logger.constants.EVAL_STOP,
                                          metadata={mlperf_logger.constants.EPOCH_NUM: epoch_num_float})

                    # Uncomment the line below to print out the total time with overhead
                    # print("Total test time for this group: {}" \
                    # .format(time_wrap(use_gpu) - accum_test_time_begin))

                    if (args.mlperf_logging
                            and (args.mlperf_acc_threshold > 0)
                            and (best_gA_test > args.mlperf_acc_threshold)):
                        print("MLPerf testing accuracy threshold "
                              + str(args.mlperf_acc_threshold)
                              + " reached, stop training", flush=True)
                        break

                    if (args.mlperf_logging
                            and (args.mlperf_auc_threshold > 0)
                            and (best_auc_test > args.mlperf_auc_threshold)):
                        print("MLPerf testing auc threshold "
                              + str(args.mlperf_auc_threshold)
                              + " reached, stop training", flush=True)
                        mlperf_logger.barrier()
                        mlperf_logger.log_end(key=mlperf_logger.constants.RUN_STOP,
                                              metadata={
                                                  mlperf_logger.constants.STATUS: mlperf_logger.constants.SUCCESS})
                        break

            mlperf_logger.barrier()
            mlperf_logger.log_end(key=mlperf_logger.constants.EPOCH_STOP,
                                  metadata={mlperf_logger.constants.EPOCH_NUM: k + 1})
            mlperf_logger.barrier()
            mlperf_logger.log_end(key=mlperf_logger.constants.BLOCK_STOP,
                                  metadata={mlperf_logger.constants.FIRST_EPOCH_NUM: k + 1})
            k += 1  # nepochs

    print("Duration: ", time.time() - start_time, flush=True)

    if args.mlperf_logging and best_auc_test <= args.mlperf_auc_threshold:
        mlperf_logger.barrier()
        mlperf_logger.log_end(key=mlperf_logger.constants.RUN_STOP,
                              metadata={mlperf_logger.constants.STATUS: mlperf_logger.constants.ABORTED})

    # profiling
    if args.enable_profiling:
        with open("dlrm_s_pytorch.prof", "w") as prof_f:
            prof_f.write(prof.key_averages().table(sort_by="cpu_time_total"))
            prof.export_chrome_trace("./dlrm_s_pytorch.json")
        # print(prof.key_averages().table(sort_by="cpu_time_total"))

    # plot compute graph
    if args.plot_compute_graph:
        sys.exit(
            "ERROR: Please install pytorchviz package in order to use the"
            + " visualization. Then, uncomment its import above as well as"
            + " three lines below and run the code again."
        )
        # V = Z.mean() if args.inference_only else E
        # dot = make_dot(V, params=dict(dlrm.named_parameters()))
        # dot.render('dlrm_s_pytorch_graph') # write .pdf file

    # test prints
    if not args.inference_only and args.debug_mode:
        print("updated parameters (weights and bias):")
        for param in dlrm.parameters():
            print(param.detach().cpu().numpy())

    # export the model in onnx
    if args.save_onnx:
        dlrm_pytorch_onnx_file = "dlrm_s_pytorch.onnx"
        torch.onnx.export(
            dlrm, (X_onnx, lS_o_onnx, lS_i_onnx), dlrm_pytorch_onnx_file, verbose=True, use_external_data_format=True
        )
        # recover the model back
        dlrm_pytorch_onnx = onnx.load("dlrm_s_pytorch.onnx")
        # check the onnx model
        onnx.checker.check_model(dlrm_pytorch_onnx)
