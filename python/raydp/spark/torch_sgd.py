from collections.abc import Iterable

import inspect

import pandas

from ray.util.sgd.utils import AverageMeterCollection
from ray.util.sgd.torch.torch_trainer import TorchTrainer

from raydp.spark.dataholder import ObjectIdList
from raydp.spark.spark_cluster import save_to_ray

import torch
from torch.nn.modules.loss import _Loss as TLoss
from torch.optim.lr_scheduler import _LRScheduler as TLRScheduler

from typing import Any, Callable, List, Optional, Union


class TorchEstimator:
    """
    A scikit-learn like API to distributed training torch model. In the backend it leverage
    the ray.sgd.TorchTrainer.

    The working flows:
        1 create the estimator instance
        2 fit on Spark DataFrame or koalas.DataFrame
        3 evaluate on Spark DataFrame or koalas.DataFrame
        4 get the model
    """
    def __init__(self,
                 num_workers: int = 1,
                 model: Union[torch.nn.Module, Callable] = None,
                 optimizer: Union[torch.optim.Optimizer, Callable] = None,
                 loss: Union[TLoss, Callable] = None,
                 lr_scheduler: Union[TLRScheduler, Callable] = None,
                 scheduler_step_freq="batch",
                 feature_columns: List[str] = None,
                 feature_shapes: Optional[List[Any]] = None,
                 label_column: str = None,
                 batch_sizes: int = None,
                 num_epochs: int = None,
                 **extra_config):
        """
        :param num_workers: the number of workers to do the distributed training
        :param model: the torch model instance or a function(dict -> Model) to create a model
        :param optimizer: the optimizer instance or a function((models, dict) -> optimizer) to
               create the optimizer in the torch.sgd.TorchTrainer
        :param loss: the loss instance or loss class or a function(dict -> loss) to create the
               loss in the torch.sgd.TorchTrainer
        :param lr_scheduler: the torch lr scheduler instance or a
               function((optimizer, config) -> lr_scheduler) to create the lr scheduler in the
               torch.sgd.TorchTrainer
        :param scheduler_step_freq: "batch", "epoch", or None. This will
               determine when ``scheduler.step`` is called. If "batch",
               ``step`` will be called after every optimizer step. If "epoch",
               ``step`` will be called after one pass of the DataLoader.
        :param feature_columns: the feature columns when fit on Spark DataFrame or koalas.DataFrame
        :param feature_shapes: the feature shapes matching the feature columns. All feature will
               be treated as a scalar value and packet into one torch.Tensor if this is not
               provided. Otherwise, each feature column will be one torch.Tensor and with the
               provided shapes.
        :param label_column: the label column when fit on Spark DataFrame or koalas.DataFrame
        :param batch_sizes: the training batch size
        :param num_epochs: the total number of epochs will be train
        :param extra_config: the extra config will be set to torch.sgd.TorchTrainer
        """
        self._num_workers = num_workers
        self._model = model
        self._optimizer = optimizer
        self._loss = loss
        self._lr_scheduler = lr_scheduler
        self._scheduler_step_freq = scheduler_step_freq
        self._feature_columns = feature_columns
        self._feature_shapes = feature_shapes
        self._label_column = label_column
        self._batch_sizes = batch_sizes
        self._num_epochs = num_epochs
        self._extra_config = extra_config
        self._data_set = None

        self._trainer = None

        self._check()

    def _check(self):
        assert self._model is not None, "Model must be provided"
        assert self._optimizer is not None, "Optimizer must be provided"
        assert self._loss is not None, "Loss must be provided"

        if self._feature_shapes:
            assert len(self._feature_columns) == len(self._feature_shapes), \
                "The feature_shapes size must match the feature_columns"

    def _create_trainer(self):
        def model_creator(config):
            if isinstance(self._model, torch.nn.Module):
                # it is the instance of torch.nn.Module
                return self._model(config)
            elif callable(self._model):
                return self._model
            else:
                raise Exception(
                    "Unsupported parameter, we only support torch.nn.Model instance "
                    "or a function(dict -> model)")

        def optimizer_creator(model, config):
            if isinstance(self._optimizer, torch.optim.Optimizer):
                # it is the instance of torch.optim.Optimizer subclass instance
                return self._optimizer
            elif callable(self._optimizer):
                return self._optimizer(model, config)
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

        def data_creator(config):
            dataloader = torch.utils.data.DataLoader(self._data_set, self._batch_sizes)
            return dataloader, None

        def scheduler_creator(optimizer, config):
            if isinstance(self._lr_scheduler, TLRScheduler):
                # it is the instance
                return self._lr_scheduler
            elif callable(self._lr_scheduler):
                self._lr_scheduler(optimizer, config)
            else:
                raise Exception(
                    "Unsupported parameter, we only support torch.optim.lr_scheduler._LRScheduler "
                    "subclass instance or a function((optimizer, dict) -> lr_scheduler)")

        lr_scheduler_creator = scheduler_creator if self._lr_scheduler is not None else None

        self._trainer = TorchTrainer(model_creator=model_creator,
                                     data_creator=data_creator,
                                     optimizer_creator=optimizer_creator,
                                     loss_creator=loss_creator,
                                     scheduler_creator=lr_scheduler_creator,
                                     scheduler_step_freq=self._scheduler_step_freq,
                                     num_workers=self._num_workers,
                                     add_dist_sampler=False,
                                     use_tqdm=True,
                                     **self._extra_config)

    def fit(self, df):
        if self._trainer is None:
            self._data_set = RayDataset(
                df, self._feature_columns, self._feature_shapes, self._label_column)
            self._create_trainer()
            assert self._trainer is not None
            for i in self._num_epochs:
                info = dict()
                info["epoch_idx"] = i
                info["num_epochs"] = self._num_epochs
                self._trainer.train(info=info)
        else:
            raise Exception("You call fit twice.")

    def evaluate(self, df):
        if self._trainer is None:
            raise Exception("Must call fit first")
        pdf = df.toPandas()
        dataset = PandasDataset(
            pdf, self._feature_columns, self._feature_shapes, self._label_column)
        dataloader = torch.utils.data.DataLoader(dataset, self._batch_sizes)
        model = self.get_model()
        model.eval()
        metric_meters = AverageMeterCollection()

        with torch.no_grad():
            for batch_idx, batch in enumerate(dataloader):
                batch_info = {"batch_idx": batch_idx}
                # unpack features into list to support multiple inputs model
                *features, target = batch
                output = self.model(*features)
                loss = self.criterion(output, target)
                _, predicted = torch.max(output.data, 1)
                num_correct = (predicted == target).sum().item()
                num_samples = target.size(0)
                metrics = {
                    "val_loss": loss.item(),
                    "val_accuracy": num_correct / num_samples,
                    "num_samples": num_samples}
                metric_meters.update(metrics)

        return metric_meters.summary()

    def get_model(self):
        assert self._trainer is None, "Must call fit first"
        return self._trainer.get_model()

    def save(self, checkpoint):
        assert self._trainer is None, "Must call fit first"
        self._trainer.save(checkpoint)

    def load(self, checkpoint):
        assert self._trainer is None, "Must call fit first"
        self._trainer.load(checkpoint)

    def shutdown(self):
        if self._trainer is not None:
            self._trainer.shutdown()
            self._trainer = None


class RayDataset(torch.utils.data.IterableDataset):
    def __init__(self,
                 df: Any = None,
                 feature_columns: List[str] = None,
                 feature_shapes: List[Any] = None,
                 label_column: str = None):
        """
        TODO: support shards
        :param df: Spark DataFrame or Koalas.DataFrame
        :param feature_columns: the feature columns in df
        :param label_column: the label column in df
        :param feature_shapes: the each feature shape that need to return when loading this
               dataset. If it is not None, it's size must match the size of feature_columns.
               If it is None, we guess all are scalar value and return all as a tensor when
               loading this dataset.
        """
        super(RayDataset, self).__init__()
        self._objs: ObjectIdList = None
        self._feature_columns = feature_columns
        self._label_column = label_column
        self._feature_shapes = feature_shapes
        self._df_index = 0
        self._index = 0
        self._feature_df = None
        self._label_df = None

        if self._feature_shapes:
            assert len(self._feature_columns) == len(self._feature_shapes),\
                "The feature_shapes size must match the feature_columns"
            for i in range(len(self._feature_shapes)):
                if not isinstance(self._feature_shapes[i], Iterable):
                    self._feature_shapes[i] = [self._feature_shapes[i]]

        if df is not None:
            self._objs = save_to_ray(df)

    def _reset(self):
        self._df_index = 0
        self._index = 0
        df = self._objs[self._df_index]
        self._feature_df = df[self._feature_columns].values
        self._label_df = df[self._label_column].values

    def __iter__(self):
        worker_info = torch.utils.data.get_worker_info()
        if worker_info:
            # TODO: add support
            raise Exception("Multiple processes loading is not supported")
        self._objs.resolve(True)
        self._reset()
        return self

    def __next__(self):
        # TODO: should this too slowly?
        if self._index >= len(self._feature_df):
            self._df_index += 1
            if self._df_index >= len(self._objs):
                raise StopIteration()
            else:
                df = self._objs[self._df_index]
                self._feature_df = df[self._feature_columns].values
                self._label_df = df[self._label_column].values
                self._index = 0

        label = torch.as_tensor(self._label_df[self._index]).view(1)
        current_feature = self._feature_df[self._index]
        if self._feature_shapes:
            feature_tensors = []
            for i, shape in enumerate(self._feature_shapes):
                feature_tensors.append(torch.as_tensor(current_feature[i]).view(*shape))
            self._index += 1
            return (*feature_tensors, label)
        else:
            feature = torch.as_tensor(current_feature)
            self._index += 1
            return feature, label

    def __len__(self):
        return self._objs.total_size

    @classmethod
    def _custom_deserialize(cls,
                            objs: ObjectIdList,
                            feature_columns: List[str],
                            label_column: str,
                            feature_shapes: List[Any]):
        dataset = cls(None, feature_columns, label_column, feature_shapes)
        dataset._objs = objs
        return dataset

    def __reduce__(self):
        return (RayDataset._custom_deserialize,
                (self._objs, self._feature_columns, self._label_column, self._feature_shapes))


class PandasDataset(torch.utils.data.Dataset):
    """
    A pandas dataset which support feature columns with different shapes.
    """
    def __init__(self,
                 df: pandas.DataFrame = None,
                 feature_columns: List[str] = None,
                 feature_shapes: List[Any] = None,
                 label_column: str = None):
        """
        :param df: pandas DataFrame
        :param feature_columns: the feature columns in the df
        :param feature_shapes: the each feature shape that need to return when loading this
               dataset. If it is not None, it's size must match the size of feature_columns.
               If it is None, we guess all are scalar value and return all as a tensor when
               loading this dataset.
        :param label_column: the label column in the df.
        """
        self._feature_df = df[feature_columns].values
        self._label_df = df[label_column].values

        self._feature_shapes = feature_shapes
        if self._feature_shapes:
            assert len(self._feature_columns) == len(self._feature_shapes), \
                "The feature_shapes size must match the feature_columns"
            for i in range(len(self._feature_shapes)):
                if not isinstance(self._feature_shapes[i], Iterable):
                    self._feature_shapes[i] = [self._feature_shapes[i]]

    def __getitem__(self, index):
        label = torch.as_tensor(self._label_df[index]).view(1)
        current_feature = self._feature_df[index]
        if self._feature_shapes:
            feature_tensors = []
            for i, shape in enumerate(self._feature_shapes):
                feature_tensors.append(torch.as_tensor(current_feature[i]).view(*shape))
            return (*feature_tensors, label)
        else:
            feature = torch.as_tensor(current_feature)
            return feature, label

