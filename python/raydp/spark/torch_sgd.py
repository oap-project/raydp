from collections.abc import Iterable

import inspect

import pandas

from ray.util.sgd.utils import AverageMeterCollection
from ray.util.sgd.torch.torch_trainer import TorchTrainer

from raydp.spark.dataholder import ObjectIdList
from raydp.spark.spark_cluster import save_to_ray

import torch
from torch.nn.modules.loss import _Loss as TLoss
from torch.utils.data import Dataset, IterableDataset

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
               provided shapes.
        :param feature_types: the feature types matching the feature columns. All feature will be
               cast into torch.float by default. Otherwise, cast into the provided type.
        :param label_column: the label column when fit on Spark DataFrame or koalas.DataFrame
        :param label_type: the label type, this will be cast into torch.float by default
        :param batch_size: the training batch size
        :param num_epochs: the total number of epochs will be train
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
        self._extra_config = extra_config
        if self._extra_config:
            if "config" in self._extra_config:
                self._extra_config["config"].update({"batch_size": self._batch_size})
            else:
                self._extra_config["config"] = {"batch_size": self._batch_size}
        else:
            self._extra_config = {"config": {"batch_size": self._batch_size}}
        self._data_set = None

        self._trainer = None

        self._check()

    def _check(self):
        assert self._model is not None, "Model must be provided"
        assert self._optimizer is not None, "Optimizer must be provided"
        assert self._loss is not None, "Loss must be provided"

        if self._feature_shapes is not None:
            assert len(self._feature_columns) == len(self._feature_shapes), \
                "The feature_shapes size must match the feature_columns"

    def _create_trainer(self):
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

        def data_creator(config):
            batch_size = config["batch_size"]
            dataloader = torch.utils.data.DataLoader(self._data_set, batch_size)
            return dataloader, None

        def scheduler_creator(optimizers, config):
            return self._lr_scheduler_creator(optimizers, config)

        lr_scheduler_creator = scheduler_creator if self._lr_scheduler_creator is not None else None

        self._trainer = TorchTrainer(model_creator=model_creator,
                                     data_creator=data_creator,
                                     optimizer_creator=optimizer_creator,
                                     loss_creator=loss_creator,
                                     scheduler_creator=lr_scheduler_creator,
                                     scheduler_step_freq=self._scheduler_step_freq,
                                     num_workers=self._num_workers,
                                     add_dist_sampler=False,
                                     **self._extra_config)

    def fit(self, df):
        if self._trainer is None:
            self._data_set = RayDataset(df, self._feature_columns, self._feature_shapes,
                                        self._feature_types, self._label_column, self._label_type)
            self._create_trainer()
            assert self._trainer is not None
            for i in range(self._num_epochs):
                info = dict()
                info["epoch_idx"] = i
                info["num_epochs"] = self._num_epochs
                stats = self._trainer.train(info=info)
                print(f"Epoch-{i}: {stats}")
        else:
            raise Exception("You call fit twice.")

    def evaluate(self, df):
        if self._trainer is None:
            raise Exception("Must call fit first")
        pdf = df.toPandas()
        dataset = PandasDataset(pdf, self._feature_columns, self._feature_shapes,
                                self._feature_types, self._label_column, self._label_type)
        dataloader = torch.utils.data.DataLoader(dataset, self._batch_size)

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
        assert self._trainer is not None, "Must call fit first"
        return self._trainer.get_model()

    def save(self, checkpoint):
        assert self._trainer is not None, "Must call fit first"
        self._trainer.save(checkpoint)

    def load(self, checkpoint):
        assert self._trainer is not None, "Must call fit first"
        self._trainer.load(checkpoint)

    def shutdown(self):
        if self._trainer is not None:
            del self._data_set
            self._trainer.shutdown()
            self._trainer = None


class _Dataset:
    def __init__(self,
                 feature_columns: List[str] = None,
                 feature_shapes: Optional[List[Any]] = None,
                 feature_types: Optional[List[torch.dtype]] = None,
                 label_column: str = None,
                 label_type: Optional[torch.dtype] = None):
        """
        :param feature_columns: the feature columns in df
        :param feature_shapes: the each feature shape that need to return when loading this
               dataset. If it is not None, it's size must match the size of feature_columns.
               If it is None, we guess all are scalar value and return all as a tensor when
               loading this dataset.
        :param feature_types: the feature types. All will be casted into torch.float by default
        :param label_column: the label column in df
        :param label_type: the label type. It will be casted into torch.float by default.
        """
        super(_Dataset, self).__init__()
        self._feature_columns = feature_columns
        self._feature_shapes = feature_shapes
        self._feature_types = feature_types
        self._label_column = label_column
        self._label_type = label_type

    def _check(self):
        if self._feature_shapes:
            assert len(self._feature_columns) == len(self._feature_shapes), \
                "The feature_shapes size must match the feature_columns"
            for i in range(len(self._feature_shapes)):
                if not isinstance(self._feature_shapes[i], Iterable):
                    self._feature_shapes[i] = [self._feature_shapes[i]]

        if self._feature_types:
            assert len(self._feature_columns) == len(self._feature_types), \
                "The feature_types size must match the feature_columns"
            for i in range(len(self._feature_types)):
                assert all(isinstance(dtype, torch.dtype) for dtype in self._feature_types),\
                    "All value in feature_types should be torch.dtype instance"

        if not self._feature_shapes and self._feature_types:
            assert all(dtype == self._feature_types[0] for dtype in self._feature_types),\
                "All dtypes should be same when feature_shapes doesn't provide"

        if not self._feature_types:
            self._feature_types = [torch.float] * len(self._feature_columns)

        if not self._label_type:
            self._label_type = torch.float

    def _get_next(self, index, feature_df, label_df):
        label = torch.as_tensor(label_df[index], dtype=self._label_type)
        current_feature = feature_df[index]
        if self._feature_shapes:
            feature_tensors = []
            for i, (shape, dtype) in enumerate(zip(self._feature_shapes, self._feature_types)):
                feature_tensors.append(
                    torch.as_tensor(current_feature[i], dtype=dtype).view(*shape))
            return (*feature_tensors, label)
        else:
            feature = torch.as_tensor(current_feature, dtype=self._feature_types[0])
            return feature, label


class RayDataset(IterableDataset, _Dataset):
    """
    Store Spark DataFrame or koalas.DataFrame into ray object store and wrap into a torch
    Dataset which could be used by torch DataLoader.

    TODO: support shards.
    """
    def __init__(self,
                 df: Any = None,
                 feature_columns: List[str] = None,
                 feature_shapes: Optional[List[Any]] = None,
                 feature_types: Optional[List[torch.dtype]] = None,
                 label_column: str = None,
                 label_type: Optional[torch.dtype] = None):
        """
        :param df: Spark DataFrame or Koalas.DataFrame
        """
        super(RayDataset, self).__init__()
        super(IterableDataset, self).__init__(feature_columns, feature_shapes,
                                              feature_types, label_column, label_type)
        self._objs: ObjectIdList = None
        self._df_index = 0
        self._index = 0
        self._feature_df = None
        self._label_df = None

        self._check()

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

        index = self._index
        self._index += 1
        return self._get_next(index, self._feature_df, self._label_df)

    def __len__(self):
        return self._objs.total_size

    @classmethod
    def _custom_deserialize(cls,
                            objs: ObjectIdList,
                            feature_columns: List[str],
                            feature_shapes: List[Any],
                            feature_types: List[torch.dtype],
                            label_column: str,
                            label_type: torch.dtype):
        dataset = cls(
            None, feature_columns, feature_shapes, feature_types, label_column, label_type)
        dataset._objs = objs
        return dataset

    def __reduce__(self):
        return (RayDataset._custom_deserialize,
                (self._objs, self._feature_columns, self._feature_shapes, self._feature_types,
                 self._label_column, self._label_type))


class PandasDataset(Dataset, _Dataset):
    """
    A pandas dataset which support feature columns with different shapes.
    """
    def __init__(self,
                 df: pandas.DataFrame = None,
                 feature_columns: List[str] = None,
                 feature_shapes: Optional[List[Any]] = None,
                 feature_types: Optional[List[torch.dtype]] = None,
                 label_column: str = None,
                 label_type: Optional[torch.dtype] = None):
        """
        :param df: pandas DataFrame
        """
        super(PandasDataset, self).__init__()
        super(Dataset, self).__init__(feature_columns, feature_shapes,
                                      feature_types, label_column, label_type)
        self._feature_df = df[feature_columns].values
        self._label_df = df[label_column].values

        self._check()

    def __getitem__(self, index):
        return self._get_next(index, self._feature_df, self._label_df)

    def __len__(self):
        return len(self._label_df)
