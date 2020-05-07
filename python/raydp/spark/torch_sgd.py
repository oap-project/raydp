from collections.abc import Iterable

import inspect

import pandas

from ray.util.sgd.torch.torch_trainer import TorchTrainer

from raydp.spark.dataholder import ObjectIdList
from raydp.spark.spark_cluster import save_to_ray

import torch

from typing import Any, Callable, List, Union


class TorchEstimator:
    def __init__(self,
                 num_workers: int = None,
                 model: Union[torch.nn.Module, Callable] = None,
                 optimizer: Union[torch.optim.Optimizer, Callable] = None,
                 loss: Union[torch.nn._Loss, Callable] = None,
                 feature_columns: List[str] = None,
                 feature_shapes: List[Any] = None,
                 label_column: str = None,
                 batch_sizes: int = None,
                 num_epochs: int = None,
                 **extra_config):
        self._num_workers = num_workers
        self._model = model
        self._optimizer = optimizer
        self._loss = loss
        self._feature_columns = feature_columns
        self._feature_shapes = feature_shapes
        self._label_column = label_column
        self._batch_sizes = batch_sizes
        self._num_epochs = num_epochs
        self._extra_config = extra_config
        self._data_set = None

        self._trainer = None

    def _create_trainer(self):
        def model_creator(config):
            if callable(self._model):
                return self._model()
            else:
                return self._model

        def optimizer_creator(models, config):
            if callable(self._optimizer):
                return self._optimizer()
            else:
                return self._optimizer

        def loss_creator(config):
            if inspect.isclass(self._loss) and issubclass(
                    self._loss, torch.nn.modules.loss._Loss):
                return self._loss
            elif callable(self._loss):
                return self._loss()
            else:
                return self._loss

        def data_creator(config):
            dataloader = torch.utils.data.DataLoader(self._data_set, self._batch_sizes)
            return dataloader, None

        self._trainer = TorchTrainer(model_creator=model_creator,
                                     data_creator=data_creator,
                                     optimizer_creator=optimizer_creator,
                                     loss_creator=loss_creator,
                                     num_workers=self._num_workers,
                                     add_dist_sampler=False,
                                     **self._extra_config)

    def fit(self, df):
        if self._trainer is not None:
            self._data_set = RayDataset(
                df, self._feature_columns, self._feature_shapes, self._label_column)
            self._create_trainer()
            assert self._trainer is not None
            for i in self._num_epochs:
                self._trainer.train()
        else:
            raise Exception("You call fit twice.")

    def evaluate(self, df):
        pdf = df.toPandas()
        dataset = PandasDataset(
            pdf, self._feature_columns, self._feature_shapes, self._label_column)
        dataloader = torch.utils.data.DataLoader(dataset, self._batch_sizes)
        model = self.get_model()
        correct = 0
        total = 0
        with torch.no_grad():
            for data in dataloader:
                features, labels = data
                outputs = model(features)
                total += labels.size(0)
                correct += (outputs == labels).sum().item()

        print('Accuracy of the network on the evaluation data: %d %%' % (
            100 * correct / total))

    def get_model(self):
        assert self._trainer is None, "Must call fit first"
        return self._trainer.get_model()

    def save(self, checkpoint):
        self._trainer.save(checkpoint)

    def load(self, checkpoint):
        self._trainer.load(checkpoint)

    def shutdown(self):
        self._trainer.shutdown()


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

