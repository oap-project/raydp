from collections.abc import Iterable

from raydp.spark.dataholder import ObjectIdList
from raydp.spark.spark_cluster import save_to_ray

import torch

from typing import Any, List


# TODO: support shards
class RayDataset(torch.utils.data.IterableDataset):
    def __init__(self,
                 df: Any,
                 feature_columns: List[str],
                 label_column: str,
                 feature_shapes: List[Any] = None):
        """
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

        label = torch.tensor(self._label_df[self._index]).view(1).to(torch.float)
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
