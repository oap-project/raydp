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
from typing import Any, Callable, List, NoReturn, Optional, Union, Dict
from numpy import outer

import torch
from torch.nn.modules.loss import _Loss as TLoss

from raydp.estimator import EstimatorInterface
from raydp.spark.interfaces import SparkEstimatorInterface, DF, OPTIONAL_DF

from ray import train
from ray.train import Trainer, TrainingCallback, get_dataset_shard
import ray.data
from ray.data.dataset import Dataset
from ray.data.impl.arrow_block import ArrowRow

class TorchEstimator(EstimatorInterface, SparkEstimatorInterface):
    """
    A scikit-learn like API to distributed training torch model. In the backend it leverage
    the ray.train.

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
                 resources_per_worker: Optional[Dict[str, float]] = None,
                 model: Union[torch.nn.Module, Callable] = None,
                 optimizer: Union[torch.optim.Optimizer, Callable] = None,
                 loss: Union[TLoss, Callable] = None,
                 lr_scheduler_creator: Optional[Callable] = None,
                 scheduler_step_freq="batch",
                 feature_columns: List[str] = None,
                 feature_types: Optional[List[torch.dtype]] = None,
                 label_column: str = None,
                 label_type: Optional[torch.dtype] = None,
                 batch_size: int = None,
                 drop_last: bool = False,
                 num_epochs: int = None,
                 shuffle: bool = False,
                 num_processes_for_data_loader: int = 0,
                 callbacks: Optional[List[TrainingCallback]] = None,
                 **extra_config):
        """
        :param num_workers: the number of workers to do the distributed training
        :param resources_per_worker: the resources defined in this Dict will be reserved for
               each worker. The ``CPU`` and ``GPU`` keys (case-sensitive) can be defined to
               override the number of CPU/GPUs used by each worker.
        :param model: the torch model instance or a function(dict -> Models) to create a model
        :param optimizer: the optimizer instance or a function((models, dict) -> optimizer) to
               create the optimizer in the ray.train.Trainer
        :param loss: the loss instance or loss class or a function(dict -> loss) to create the
               loss in the ray.train.Trainer
        :param lr_scheduler_creator: a function((optimizers, config) -> lr_scheduler) to create
               the lr scheduler
        :param scheduler_step_freq: "batch", "epoch", or None. This will
               determine when ``scheduler.step`` is called. If "batch",
               ``step`` will be called after every optimizer step. If "epoch",
               ``step`` will be called after one pass of the DataLoader.
               Note: Current code only supports "batch"
        :param feature_columns: the feature columns when fit on Spark DataFrame or koalas.DataFrame.
               The inputs of the model will be match the feature columns.
               .. code-block:: python
                   feature_columns = ["x", "y", "z"]
                   # the input to the model will be [x_batch_tensor, y_batch_tensor, z_batch_tensor]
        :param feature_types: the feature types matching the feature columns. All feature will be
               cast into torch.float by default. Otherwise, cast into the provided type.
        :param label_column: the label column when fit on Spark DataFrame or koalas.DataFrame
        :param label_type: the label type, this will be cast into torch.float by default
        :param batch_size: the training batch size
        :param drop_last: Set to True to drop the last incomplete batch
        :param num_epochs: the total number of epochs will be train
        :param shuffle: whether shuffle the data
               Note: Now the value can only be False
        :param num_processes_for_data_loader: the number of processes use to speed up data loading
        :param callbacks: which will be executed during training.
        :param extra_config: the extra config will be set to ray.train.Trainer
        """
        self._num_workers = num_workers
        self._resources_per_worker = resources_per_worker
        self._model = model
        self._optimizer = optimizer
        self._loss = loss
        self._lr_scheduler_creator = lr_scheduler_creator
        self._feature_columns = feature_columns
        self._feature_types = feature_types
        self._label_column = label_column
        self._label_type = label_type
        self._batch_size = batch_size
        self._drop_last = drop_last
        self._num_epochs = num_epochs
        self._num_processes_for_data_loader = num_processes_for_data_loader
        self._callbacks = callbacks
        self._extra_config = extra_config

        if self._num_processes_for_data_loader > 0:
            raise TypeError("multiple processes for data loader has not supported")

        self._trainer: Trainer = None

        self._check()

    def _check(self):
        assert self._model is not None, "Model must be provided"
        assert self._optimizer is not None, "Optimizer must be provided"
        assert self._loss is not None, "Loss must be provided"

    @staticmethod
    def train_fun(config):
        # create model
        if isinstance(config["model"], torch.nn.Module):
            model = config["model"]
        elif callable(config["model"]):
            model = config["model"](config)
        else:
            raise Exception(
                "Unsupported parameter, we only support torch.nn.Model instance "
                "or a function(dict -> model)")

        # create optimizer
        if isinstance(config["optimizer"], torch.optim.Optimizer):
            # it is the instance of torch.optim.Optimizer subclass instance
            # rewrite the optimizer
            optimizer_cls = config["optimizer"].__class__
            state = config["optimizer"].state_dict()
            optimizer = optimizer_cls(model.parameters(), lr=0.1)  # lr must pass for SGD
            optimizer.load_state_dict(state)
        elif callable(config["optimizer"]):
            optimizer = config["optimizer"](model, config)
        else:
            raise Exception(
                "Unsupported parameter, we only support torch.optim.Optimizer subclass "
                "instance or a function((models, dict) -> optimizer)")

        # create loss
        if inspect.isclass(config["loss"]) and issubclass(config["loss"], TLoss):
            loss = config["loss"]
        elif isinstance(config["loss"], TLoss):
            loss = config["loss"]
        elif callable(config["loss"]):
            loss = config["loss"](config)
        else:
            raise Exception(
                "Unsupported parameter, we only support torch.nn.modules.loss._Loss "
                "subclass, subclass instance or a function(dict -> loss)")

        # create lr scheduler
        if config["lr_scheduler_creator"]:
            lr_scheduler = config["lr_scheduler_creator"](optimizer, config)
        else:
            lr_scheduler = None

        # create dataset
        train_data_shard = get_dataset_shard("train")
        train_dataset = train_data_shard.to_torch(feature_columns=config["feature_columns"],
                                                feature_column_dtypes=config["feature_types"],
                                                label_column=config["label_column"],
                                                label_column_dtype=config["label_type"],
                                                batch_size=config["batch_size"],
                                                drop_last=config["drop_last"])
        if config["evaluate"]:
            evaluate_data_shard = get_dataset_shard("evaluate")
            evaluate_dataset = evaluate_data_shard.to_torch(
                                                    feature_columns=config["feature_columns"],
                                                    label_column=config["label_column"],
                                                    label_column_dtype=config["label_type"],
                                                    feature_column_dtypes=config["feature_types"],
                                                    batch_size=config["batch_size"],
                                                    drop_last=config["drop_last"])

        model = train.torch.prepare_model(model)
        loss_results = []
        for epoch in range(config["num_epochs"]):
            train_acc, train_loss = TorchEstimator.train_epoch(train_dataset, model, loss,
                                                                optimizer, lr_scheduler)
            train.report(epoch=epoch, train_acc=train_acc, train_loss=train_loss)
            if config["evaluate"]:
                evaluate_acc, evaluate_loss = TorchEstimator.evaluate_epoch(evaluate_dataset,
                                                                            model, loss)
                train.report(epoch=epoch, evaluate_acc=evaluate_acc, test_loss=evaluate_loss)
                loss_results.append(evaluate_loss)

    @staticmethod
    def train_epoch(dataset, model, criterion, optimizer, scheduler=None):
        model.train()
        train_loss, correct, data_size, batch_idx = 0, 0, 0, 0
        for batch_idx, (inputs, targets) in enumerate(dataset):
            # Compute prediction error
            inputs = [inputs[:,i].unsqueeze(1) for i in range(inputs.size(1))]
            outputs = model(*inputs)
            loss = criterion(outputs, targets)
            train_loss += loss.item()
            correct += (outputs == targets).sum().item()
            data_size += targets.size(0)
            # Backpropagation
            optimizer.zero_grad()
            loss.backward()
            optimizer.step()
            if scheduler is not None:
                scheduler.step()

        train_loss /= (batch_idx + 1)
        train_acc = correct/data_size
        return train_acc, train_loss

    @staticmethod
    def evaluate_epoch(dataset, model, criterion):
        model.eval()
        test_loss, correct, data_size, batch_idx = 0, 0, 0, 0
        with torch.no_grad():
            for batch_idx, (inputs, targets) in enumerate(dataset):
                # Compute prediction error
                inputs = [inputs[:,i].unsqueeze(1) for i in range(inputs.size(1))]
                outputs = model(*inputs)
                test_loss += criterion(outputs, targets).item()
                correct += (outputs == targets).sum().item()
                data_size += targets.size(0)
        test_loss /= (batch_idx + 1)
        test_acc = correct/data_size
        return test_acc, test_loss

    def fit(self,
            train_ds: Dataset[ArrowRow],
            evaluate_ds: Optional[Dataset[ArrowRow]] = None,
            max_retries=3) -> NoReturn:
        super().fit(train_ds, evaluate_ds)

        class PrintingCallback(TrainingCallback):
            def handle_result(self, results: List[Dict], **info):
                print(results)

        self._trainer = Trainer(backend="torch", num_workers=self._num_workers,
                                resources_per_worker=self._resources_per_worker,
                                max_retries=max_retries, **self._extra_config)

        config = {"num_workers": self._num_workers, "model": self._model,
                  "optimizer": self._optimizer, "loss": self._loss,
                  "lr_scheduler_creator": self._lr_scheduler_creator,
                  "feature_columns": self._feature_columns, "feature_types": self._feature_types,
                  "label_column": self._label_column, "label_type": self._label_type,
                  "batch_size": self._batch_size, "num_epochs": self._num_epochs,
                  "drop_last": self._drop_last, "evaluate": True}
        dataset = {"train": train_ds}
        if evaluate_ds is None:
            config["evaluate"] = False
        else:
            dataset["evaluate"] = evaluate_ds
        self._trainer.start()
        results = self._trainer.run(
            TorchEstimator.train_fun, config=config,
            callbacks=self._callbacks,
            dataset=dataset
        )
        return results

    def fit_on_spark(self,
                     train_df: DF,
                     evaluate_df: OPTIONAL_DF = None,
                     max_retries=3):
        super().fit_on_spark(train_df, evaluate_df)
        train_df = self._check_and_convert(train_df)
        if evaluate_df is not None:
            evaluate_df = self._check_and_convert(evaluate_df)
        train_ds = ray.data.from_spark(train_df, parallelism=self._num_workers)
        evaluate_ds = None
        if evaluate_df is not None:
            evaluate_ds = ray.data.from_spark(evaluate_df, parallelism=self._num_workers)
        return self.fit(
            train_ds, evaluate_ds, max_retries)

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
