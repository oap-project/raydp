#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from ray.util.sgd.torch.training_operator import TrainingOperator

from raydp.torch import TorchDataset


class TrainingOperatorWithWarmUp(TrainingOperator):
    def __init__(self,
                 config,
                 models,
                 optimizers,
                 train_loader,
                 validation_loader,
                 world_rank,
                 criterion=None,
                 schedulers=None,
                 device_ids=None,
                 use_gpu=False,
                 use_fp16=False,
                 use_tqdm=False):
        super(TrainingOperatorWithWarmUp, self).__init__(
            config=config, models=models, optimizers=optimizers, train_loader=train_loader,
            validation_loader=validation_loader, world_rank=world_rank, criterion=criterion,
            schedulers=schedulers, device_ids=device_ids, use_gpu=use_gpu, use_fp16=use_fp16,
            use_tqdm=use_tqdm)

    def setup(self, config):
        """We trigger the underlying object transfer before training startup"""
        dataset = self.train_loader.dataset
        if isinstance(dataset, TorchDataset):
            dataset.set_rank(self.world_rank)

    def train_epoch(self, iterator, info):
        if self.timers is not None:
            iterator = self._profile_data_loader(iterator)
        return super(TrainingOperatorWithWarmUp, self).train_epoch(iterator, info)

    def _profile_data_loader(self, iterator):
        outer = self

        class DataLoaderWithProfile:
            def __init__(self, iterator):
                self._iterator = iterator
                self._underlying_iterator = None

            def __iter__(self):
                if self._underlying_iterator is None:
                    self._underlying_iterator = iter(self._iterator)
                return self

            def __next__(self):
                with outer.timers.record("data_load"):
                    return next(self._underlying_iterator)

            def __len__(self):
                return len(iterator)

        return DataLoaderWithProfile(iterator)
