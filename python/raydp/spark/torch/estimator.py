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

import inspect
from typing import Any, Callable, List, NoReturn, Optional, Union

import setproctitle
import torch
from ray.util.sgd.torch.torch_trainer import TorchTrainer
from ray.util.sgd.utils import AverageMeterCollection
from torch.nn.modules.loss import _Loss as TLoss

from raydp.dataset import Dataset
from raydp.estimator import EstimatorInterface
from raydp.spark.interfaces import SparkEstimatorInterface
from raydp.spark.torch.dataset import BlockSetSampler, PandasDataset, RayDataset
from raydp.spark.torch.operator import TrainingOperatorWithWarmUp


def worker_init_fn(work_id):
    """This must at top level"""
    worker_info = torch.utils.data.get_worker_info()
    num_workers = worker_info.num_workers
    title = f"data_loading_process_{num_workers}_{work_id}"
    setproctitle.setproctitle(title)


class TorchEstimator(EstimatorInterface, SparkEstimatorInterface):
    """
    A scikit-learn like API to distributed training torch model. In the backend it leverage
    the ray.sgd.TorchTrainer.

    The working flows:
        1 create the estimator instance
        2 fit on Spark DataFrame or koalas.DataFrame
        3 evaluate on Spark DataFrame or koalas.DataFrame
        4 get the model

    Note:
        You should pass the callable function if you want to train multiple modules. eg:
        .. code-block:: python

           def model_creator(config):
               ...
               return model1, model2

           def optimizer_creator(models, config):
               ...
               return opt1, opt2

           def scheduler_creator(optimizers, config):
               ...
               return scheduler

           estimator = TorchEstimator(num_workers=2,
                                      model=model_creator,
                                      optimizer=optimizer_creator,
                                      loss=torch.nn.MSELoss,
                                      lr_scheduler=scheduler_creator)
           estimator.fit(train_df)
           estimator.evaluate(test_df)

    """
    def __init__(self,
                 num_workers: int = 1,
                 model: Union[torch.nn.Module, Callable] = None,
                 optimizer: Union[torch.optim.Optimizer, Callable] = None,
                 loss: Union[TLoss, Callable] = None,
                 lr_scheduler_creator: Optional[Callable] = None,
                 scheduler_step_freq="batch",
                 feature_columns: List[str] = None,
                 feature_shapes: Optional[List[Any]] = None,
                 feature_types: Optional[List[torch.dtype]] = None,
                 label_column: str = None,
                 label_type: Optional[torch.dtype] = None,
                 batch_size: int = None,
                 num_epochs: int = None,
                 shuffle: bool = True,
                 num_processes_for_data_loader: int = 0,
                 **extra_config):
        """
        :param num_workers: the number of workers to do the distributed training
        :param model: the torch model instance or a function(dict -> Models) to create a model
        :param optimizer: the optimizer instance or a function((models, dict) -> optimizer) to
               create the optimizer in the torch.sgd.TorchTrainer
        :param loss: the loss instance or loss class or a function(dict -> loss) to create the
               loss in the torch.sgd.TorchTrainer
        :param lr_scheduler_creator: a function((optimizers, config) -> lr_scheduler) to create
               the lr scheduler
        :param scheduler_step_freq: "batch", "epoch", or None. This will
               determine when ``scheduler.step`` is called. If "batch",
               ``step`` will be called after every optimizer step. If "epoch",
               ``step`` will be called after one pass of the DataLoader.
        :param feature_columns: the feature columns when fit on Spark DataFrame or koalas.DataFrame
        :param feature_shapes: the feature shapes matching the feature columns. All feature will
               be treated as a scalar value and packet into one torch.Tensor if this is not
               provided. Otherwise, each feature column will be one torch.Tensor and with the
               provided shapes (0 means scalar tensor.).
               .. code-block:: python

                   feature_columns = ["a", "b", "c"]

                   # All feature will be treated as a scalar value and packet into one torch.Tensor
                   feature_shapes = None # torch.Size([3])

                   # reshape to given type
                   feature_shapes = [5, 1, 1] # (torch.Size([5]), torch.Size([1]), torch.Size([1]))
                   feature_shapes = [5, 0, 0] # (torch.Size([5]), torch.Size(), torch.Size())

        :param feature_types: the feature types matching the feature columns. All feature will be
               cast into torch.float by default. Otherwise, cast into the provided type.
        :param label_column: the label column when fit on Spark DataFrame or koalas.DataFrame
        :param label_type: the label type, this will be cast into torch.float by default
        :param batch_size: the training batch size
        :param num_epochs: the total number of epochs will be train
        :param shuffle: whether shuffle the data
        :param num_processes_for_data_loader: the number of processes use to speed up data loading
        :param extra_config: the extra config will be set to torch.sgd.TorchTrainer
        """
        self._num_workers = num_workers
        self._model = model
        self._optimizer = optimizer
        self._loss = loss
        self._lr_scheduler_creator = lr_scheduler_creator
        self._scheduler_step_freq = scheduler_step_freq
        self._feature_columns = feature_columns
        self._feature_shapes = feature_shapes
        self._feature_types = feature_types
        self._label_column = label_column
        self._label_type = label_type
        self._batch_size = batch_size
        self._num_epochs = num_epochs
        self._shuffle = shuffle
        self._num_processes_for_data_loader = num_processes_for_data_loader
        self._extra_config = extra_config

        config = {"batch_size": self._batch_size, "shuffle": self._shuffle}
        if self._extra_config:
            if "config" in self._extra_config:
                self._extra_config["config"].update(config)
            else:
                self._extra_config["config"] = config
        else:
            self._extra_config = {"config": config}

        self._data_set = None

        self._trainer: TorchTrainer = None

        self._check()

    def _check(self):
        assert self._model is not None, "Model must be provided"
        assert self._optimizer is not None, "Optimizer must be provided"
        assert self._loss is not None, "Loss must be provided"

        if self._feature_shapes is not None:
            assert len(self._feature_columns) == len(self._feature_shapes), \
                "The feature_shapes size must match the feature_columns"

    def _create_trainer(self, data_creator: Callable):
        def model_creator(config):
            if isinstance(self._model, torch.nn.Module):
                # it is the instance of torch.nn.Module
                return self._model
            elif callable(self._model):
                return self._model(config)
            else:
                raise Exception(
                    "Unsupported parameter, we only support torch.nn.Model instance "
                    "or a function(dict -> model)")

        def optimizer_creator(models, config):
            if isinstance(self._optimizer, torch.optim.Optimizer):
                # it is the instance of torch.optim.Optimizer subclass instance
                if not isinstance(models, torch.nn.Module):
                    raise Exception(
                        "You should pass optimizers with a function((models, dict) -> optimizers) "
                        "when train with multiple models.")

                # rewrite the optimizer
                optimizer_cls = self._optimizer.__class__
                state = self._optimizer.state_dict()
                optimizer = optimizer_cls(models.parameters(), lr=0.1)  # lr must pass for SGD
                optimizer.load_state_dict(state)
                return optimizer
            elif callable(self._optimizer):
                return self._optimizer(models, config)
            else:
                raise Exception(
                    "Unsupported parameter, we only support torch.optim.Optimizer subclass "
                    "instance or a function((models, dict) -> optimizer)")

        def loss_creator(config):
            if inspect.isclass(self._loss) and issubclass(self._loss, TLoss):
                # it is the loss class
                return self._loss
            elif isinstance(self._loss, TLoss):
                # it is the loss instance
                return self._loss
            elif callable(self._loss):
                # it ts the loss create function
                return self._loss(config)
            else:
                raise Exception(
                    "Unsupported parameter, we only support torch.nn.modules.loss._Loss subclass "
                    ", subclass instance or a function(dict -> loss)")

        def scheduler_creator(optimizers, config):
            return self._lr_scheduler_creator(optimizers, config)

        lr_scheduler_creator = (scheduler_creator
                                if self._lr_scheduler_creator is not None else None)

        self._trainer = TorchTrainer(model_creator=model_creator,
                                     data_creator=data_creator,
                                     optimizer_creator=optimizer_creator,
                                     loss_creator=loss_creator,
                                     scheduler_creator=lr_scheduler_creator,
                                     scheduler_step_freq=self._scheduler_step_freq,
                                     num_workers=self._num_workers,
                                     add_dist_sampler=False,
                                     training_operator_cls=TrainingOperatorWithWarmUp,
                                     **self._extra_config)

    def fit(self, ds: Dataset, **kwargs) -> NoReturn:
        pass

    def fit_on_spark(self,
                     df,
                     num_steps=None,
                     profile=False,
                     reduce_results=True,
                     max_retries=3,
                     info=None):
        super(TorchEstimator, self).fit_on_spark(df)
        if self._trainer is None:
            self._data_set = RayDataset(df, self._feature_columns, self._feature_shapes,
                                        self._feature_types, self._label_column, self._label_type)

            def data_creator(config):
                batch_size = config["batch_size"]
                shuffle = config["shuffle"]
                sampler = BlockSetSampler(self._data_set, shuffle=shuffle)
                context = None
                init_fn = None
                if self._num_processes_for_data_loader > 0:
                    context = torch.multiprocessing.get_context("spawn")
                    init_fn = worker_init_fn

                dataloader = torch.utils.data.DataLoader(
                    self._data_set,
                    batch_size=batch_size,
                    sampler=sampler,
                    num_workers=self._num_processes_for_data_loader,
                    multiprocessing_context=context,
                    worker_init_fn=init_fn)
                return dataloader, None

            self._create_trainer(data_creator)
            assert self._trainer is not None
            for i in range(self._num_epochs):
                stats = self._trainer.train(
                    num_steps=num_steps,
                    profile=profile,
                    reduce_results=reduce_results,
                    max_retries=max_retries,
                    info=info)
                print(f"Epoch-{i}: {stats}")
        else:
            raise Exception("You call fit twice.")

    def evaluate(self, df: Dataset, **kwargs) -> NoReturn:
        pass

    def evaluate_on_spark(self, df, **kwargs):
        super(TorchEstimator, self).evaluate_on_spark(df)
        if self._trainer is None:
            raise Exception("Must call fit first")
        pdf = df.toPandas()
        dataset = PandasDataset(pdf, self._feature_columns, self._feature_shapes,
                                self._feature_types, self._label_column, self._label_type)
        dataloader = torch.utils.data.DataLoader(dataset, self._batch_size, shuffle=self._shuffle)

        if inspect.isclass(self._loss) and issubclass(self._loss, TLoss):
            # it is the loss class
            criterion = self._loss()
        elif isinstance(self._loss, TLoss):
            # it is the loss instance
            criterion = self._loss
        elif callable(self._loss):
            # it ts the loss create function
            criterion = self._loss({})

        model = self.get_model()
        model.eval()
        metric_meters = AverageMeterCollection()

        with torch.no_grad():
            for batch_idx, batch in enumerate(dataloader):
                batch_info = {"batch_idx": batch_idx}
                # unpack features into list to support multiple inputs model
                *features, target = batch
                output = model(*features)
                loss = criterion(output, target)
                num_samples = target.size(0)
                metrics = {
                    "val_loss": loss.item(),
                    "num_samples": num_samples}
                metric_meters.update(metrics)

        return metric_meters.summary()

    def get_model(self):
        assert self._trainer is not None, "Must call fit first"
        return self._trainer.get_model()

    def save(self, checkpoint):
        assert self._trainer is not None, "Must call fit first"
        self._trainer.save(checkpoint)

    def restore(self, checkpoint):
        assert self._trainer is not None, "Must call fit first"
        self._trainer.load(checkpoint)

    def shutdown(self):
        if self._trainer is not None:
            del self._data_set
            self._trainer.shutdown()
            self._trainer = None
