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

import torch
from ray.util.data import MLDataset
from ray.util.sgd.torch.torch_dataset import TorchMLDataset
from ray.util.sgd.torch.torch_trainer import TorchTrainer
from ray.util.sgd.torch.training_operator import TrainingOperator
from torch.nn.modules.loss import _Loss as TLoss
from torch.utils.data.dataloader import DataLoader

from raydp.estimator import EstimatorInterface
from raydp.spark import RayMLDataset
from raydp.spark.interfaces import SparkEstimatorInterface, DF, OPTIONAL_DF


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
           estimator.fit_on_spark(train_df, test_df)

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
                 label_shape: Optional[int] = None,
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
        :param feature_columns: the feature columns when fit on Spark DataFrame or koalas.DataFrame.
               The inputs of the model will be match the feature columns.
               .. code-block:: python
                   feature_columns = ["x", "y", "z"]
                   # the input to the model will be [x_batch_tensor, y_batch_tensor, z_batch_tensor]
        :param feature_shapes: the feature shapes matching the feature columns.
        :param feature_types: the feature types matching the feature columns. All feature will be
               cast into torch.float by default. Otherwise, cast into the provided type.
        :param label_column: the label column when fit on Spark DataFrame or koalas.DataFrame
        :param label_shape: the label shape.
        :param label_type: the label type, this will be cast into torch.float by default
        :param batch_size: the training batch size
        :param num_epochs: the total number of epochs will be train
        :param shuffle: whether shuffle the data
        :param num_processes_for_data_loader: the number of processes use to speed up data loading
        :param extra_config: the extra config will be set to torch.sgd.TorchTrainer. You can also
               set the get_shard config with
               {"config": {"get_shard": {batch_ms=0, num_async=5, shuffle_buffer_size=2, seed=0}}}.
               You can refer to the MLDataset.get_repeatable_shard for the parameters.
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
        self._label_shape = label_shape
        self._label_type = label_type
        self._batch_size = batch_size
        self._num_epochs = num_epochs
        self._shuffle = shuffle
        self._num_processes_for_data_loader = num_processes_for_data_loader
        self._extra_config = extra_config

        if self._num_processes_for_data_loader > 0:
            raise TypeError("multiple processes for data loader has not supported")

        config = {"batch_size": self._batch_size, "shuffle": self._shuffle}
        if self._extra_config:
            if "config" in self._extra_config:
                self._extra_config["config"].update(config)
            else:
                self._extra_config["config"] = config
        else:
            self._extra_config = {"config": config}

        self._trainer: TorchTrainer = None

        self._check()

    def _check(self):
        assert self._model is not None, "Model must be provided"
        assert self._optimizer is not None, "Optimizer must be provided"
        assert self._loss is not None, "Loss must be provided"

        if self._feature_shapes is not None:
            assert len(self._feature_columns) == len(self._feature_shapes), \
                "The feature_shapes size must match the feature_columns"

    def _create_trainer(self, train_ds: TorchMLDataset, evaluate_ds: Optional[TorchMLDataset]):
        outer = self

        class TorchEstimatorOperator(TrainingOperator):

            def setup(self, config):
                # create model
                if isinstance(outer._model, torch.nn.Module):
                    model = outer._model
                elif callable(outer._model):
                    model = outer._model(config)
                else:
                    raise Exception(
                        "Unsupported parameter, we only support torch.nn.Model instance "
                        "or a function(dict -> model)")

                # create optimizer
                if isinstance(outer._optimizer, torch.optim.Optimizer):
                    # it is the instance of torch.optim.Optimizer subclass instance
                    # rewrite the optimizer
                    optimizer_cls = outer._optimizer.__class__
                    state = outer._optimizer.state_dict()
                    optimizer = optimizer_cls(model.parameters(), lr=0.1)  # lr must pass for SGD
                    optimizer.load_state_dict(state)
                elif callable(outer._optimizer):
                    optimizer = outer._optimizer(model, config)
                else:
                    raise Exception(
                        "Unsupported parameter, we only support torch.optim.Optimizer subclass "
                        "instance or a function((models, dict) -> optimizer)")

                # create loss
                if inspect.isclass(outer._loss) and issubclass(outer._loss, TLoss):
                    loss = outer._loss
                elif isinstance(outer._loss, TLoss):
                    loss = outer._loss
                elif callable(outer._loss):
                    loss = outer._loss(config)
                else:
                    raise Exception(
                        "Unsupported parameter, we only support torch.nn.modules.loss._Loss "
                        "subclass, subclass instance or a function(dict -> loss)")

                # create lr scheduler
                if outer._lr_scheduler_creator:
                    lr_scheduler = outer._lr_scheduler_creator(optimizer, config)
                else:
                    lr_scheduler = None

                registered = self.register(
                    models=model, optimizers=optimizer, criterion=loss, schedulers=lr_scheduler)
                if lr_scheduler is not None:
                    self.model, self.optimizer, self.criterion, self.scheduler = registered
                else:
                    self.model, self.optimizer, self.criterion = registered

                # create dataset
                batch_size = config["batch_size"]
                get_shard_config = config.get("get_shard", {})
                if "shuffle" in config:
                    get_shard_config["shuffle"] = config["shuffle"]
                if not self._is_distributed:
                    world_rank = -1
                else:
                    world_rank = self.world_rank
                train_data = train_ds.get_shard(world_rank, **get_shard_config)
                train_loader = DataLoader(train_data, batch_size=batch_size)

                if evaluate_ds is not None:
                    evaluate_data = evaluate_ds.get_shard(self.world_rank, **get_shard_config)
                    evaluate_loader = DataLoader(evaluate_data, batch_size=batch_size)
                else:
                    evaluate_loader = None

                self.register_data(train_loader=train_loader, validation_loader=evaluate_loader)

        self._trainer = TorchTrainer(num_workers=self._num_workers,
                                     training_operator_cls=TorchEstimatorOperator,
                                     add_dist_sampler=False,
                                     scheduler_step_freq=self._scheduler_step_freq,
                                     **self._extra_config)

    def _create_tf_ds(self, ds: MLDataset) -> TorchMLDataset:
        return ds.to_torch(self._feature_columns,
                           self._feature_shapes,
                           self._feature_types,
                           self._label_column,
                           self._label_shape,
                           self._label_type)

    def fit(self,
            train_ds: MLDataset,
            evaluate_ds: Optional[MLDataset] = None,
            num_steps=None,
            profile=False,
            reduce_results=True,
            max_retries=3,
            info=None) -> NoReturn:
        super().fit(train_ds, evaluate_ds)
        train_ds = train_ds.batch(self._batch_size)
        train_tf_ds = self._create_tf_ds(train_ds)

        if evaluate_ds is not None:
            evaluate_ds = evaluate_ds.batch(self._batch_size)
            evaluate_tf_ds = self._create_tf_ds(evaluate_ds)
        else:
            evaluate_tf_ds = None

        self._create_trainer(train_tf_ds, evaluate_tf_ds)
        assert self._trainer is not None
        for i in range(self._num_epochs):
            stats = self._trainer.train(
                num_steps=num_steps,
                profile=profile,
                reduce_results=reduce_results,
                max_retries=max_retries,
                info=info)
            print(f"Epoch-{i}: {stats}")

        if evaluate_tf_ds is not None:
            print(self._trainer.validate(num_steps, profile, reduce_results, info))

    def fit_on_spark(self,
                     train_df: DF,
                     evaluate_df: OPTIONAL_DF = None,
                     fs_directory: Optional[str] = None,
                     compression: Optional[str] = None,
                     num_steps=None,
                     profile=False,
                     reduce_results=True,
                     max_retries=3,
                     info=None):
        super().fit_on_spark(train_df, evaluate_df)
        train_df = self._check_and_convert(train_df)
        if evaluate_df is not None:
            evaluate_df = self._check_and_convert(evaluate_df)
        train_ds = RayMLDataset.from_spark(
            train_df, self._num_workers, self._shuffle, None, fs_directory, compression)
        evaluate_ds = None
        if evaluate_df is not None:
            evaluate_ds = RayMLDataset.from_spark(
                evaluate_df, self._num_workers, self._shuffle, None, fs_directory, compression)
        return self.fit(
            train_ds, evaluate_ds, num_steps, profile, reduce_results, max_retries, info)

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
            self._trainer.shutdown()
            self._trainer = None
