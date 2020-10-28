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

from collections.abc import Iterable
from typing import Any, List, Optional

import numpy as np
import pandas as pd
import torch
from torch.utils.data import Dataset, IterableDataset

from raydp.parallel import PandasDataset as ParallelPandasDataset


class AbstractDataset(object):
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
        self._feature_columns = feature_columns
        self._feature_shapes = feature_shapes
        self._feature_types = feature_types
        self._label_column = label_column
        self._label_type = label_type

    def _check_and_convert(self):
        # convert to list for convenience
        if not isinstance(self._feature_columns, list):
            self._feature_columns = [self._feature_columns]

        if self._feature_shapes:
            if not isinstance(self._feature_shapes, list):
                self._feature_shapes = [self._feature_shapes]

            assert len(self._feature_columns) == len(self._feature_shapes), \
                "The feature_shapes size must match the feature_columns"
            for i in range(len(self._feature_shapes)):
                if not isinstance(self._feature_shapes[i], Iterable):
                    self._feature_shapes[i] = [self._feature_shapes[i]]

        if self._feature_types:
            if not isinstance(self._feature_types, list):
                self._feature_types = [self._feature_types]

            assert len(self._feature_columns) == len(self._feature_types), \
                "The feature_types size must match the feature_columns"
            for i in range(len(self._feature_types)):
                assert all(isinstance(dtype, torch.dtype) for dtype in self._feature_types), \
                    "All value in feature_types should be torch.dtype instance"

        if not self._feature_shapes and self._feature_types:
            assert all(dtype == self._feature_types[0] for dtype in self._feature_types), \
                "All dtypes should be same when feature_shapes doesn't provide"

        if not self._feature_types:
            self._feature_types = [torch.float] * len(self._feature_columns)

        if self._label_column is not None and not self._label_type:
            self._label_type = torch.float

    def _convert_to_tensor(self, df):
        if self._feature_shapes:
            tensors = []
            for col, shape, dtype in zip(self._feature_columns, self._feature_shapes,
                                         self._feature_types):
                column = df[col].values
                if column.dtype == np.object:
                    if isinstance(column[0], np.ndarray):
                        column = np.stack(column)
                    elif isinstance(column[0], (list, tuple)):
                        column = list(column)
                    else:
                        raise Exception(
                            f"Column {col}'s type: {type(column[0])} is not supported. It must "
                            "be numpy built in type or numpy object of (ndarray, list, tuple)")

                t = torch.as_tensor(column, dtype=dtype)
                if shape != [0]:
                    t = t.view(*(-1, *shape))
                tensors.append(t)
            feature_tensor = tensors
        else:
            feature_columns = (self._feature_columns if
                               len(self._feature_columns) > 1 else self._feature_columns[0])
            feature_df = df[feature_columns].values
            t = torch.as_tensor(feature_df, dtype=self._feature_types[0])
            feature_tensor = [t]

        label_tensor = None
        if self._label_column is not None:
            label_df = df[self._label_column].values
            label_tensor = torch.as_tensor(label_df, dtype=self._label_type)
        return feature_tensor, label_tensor


class TorchDataset(IterableDataset):
    """
    Store Spark DataFrame or koalas.DataFrame into ray object store and wrap into a torch
    Dataset which could be used by torch DataLoader.
    """
    def __init__(self,
                 parallel_pandas_ds: ParallelPandasDataset = None,
                 feature_columns: List[str] = None,
                 feature_shapes: Optional[List[Any]] = None,
                 feature_types: Optional[List[torch.dtype]] = None,
                 label_column: str = None,
                 label_type: Optional[torch.dtype] = None):

        self._parallel_pandas_ds: ParallelPandasDataset = parallel_pandas_ds
        self._feature_columns = feature_columns
        self._feature_shapes = feature_shapes
        self._feature_types = feature_types
        self._label_column = label_column
        self._label_type = label_type
        self._rank: Optional[int] = None

    def set_rank(self, rank_id: int):
        self._rank = rank_id

    def __iter__(self):
        if self._rank is None:
            it = self._parallel_pandas_ds.collect()
        else:
            it = self._parallel_pandas_ds.get_shard(self._rank, batch_size=256)
        ds = TorchIterablePandasDataset(it=it,
                                        feature_columns=self._feature_columns,
                                        feature_shapes=self._feature_shapes,
                                        feature_types=self._feature_types,
                                        label_column=self._label_column,
                                        label_type=self._label_type)
        return iter(ds)


class TorchIterablePandasDataset(AbstractDataset, IterableDataset):
    def __init__(self,
                 it: Iterable,
                 feature_columns: List[str] = None,
                 feature_shapes: Optional[List[Any]] = None,
                 feature_types: Optional[List["torch.dtype"]] = None,
                 label_column: str = None,
                 label_type: Optional["torch.dtype"] = None):
        super(TorchIterablePandasDataset, self).__init__(feature_columns, feature_shapes,
                                                         feature_types, label_column, label_type)
        self._check_and_convert()
        self._it = it

    def __iter__(self) -> Iterable:
        for pdf in self._it:
            num_rows = pdf.shape[0]
            feature_tensor, label_tensor = self._convert_to_tensor(pdf)
            for i in range(num_rows):
                features = [tensor[i] for tensor in feature_tensor]
                if label_tensor is None:
                    yield (*features,)
                else:
                    label = label_tensor[i]
                    yield (*features, label)


class TorchPandasDataset(AbstractDataset, Dataset):
    """
    A pandas dataset which support feature columns with different shapes.
    """
    def __init__(self,
                 df: pd.DataFrame = None,
                 feature_columns: List[str] = None,
                 feature_shapes: Optional[List[Any]] = None,
                 feature_types: Optional[List[torch.dtype]] = None,
                 label_column: str = None,
                 label_type: Optional[torch.dtype] = None):
        """
        :param df: pandas DataFrame
        """
        super(TorchPandasDataset, self).__init__(feature_columns, feature_shapes,
                                                 feature_types, label_column, label_type)
        self._check_and_convert()

        self._df = df
        self._size = len(df)
        self._feature_tensor = None
        self._label_tensor = None

    def _get_next(self, index):
        if self._feature_tensor is None:
            self._feature_tensor, self._label_tensor = self._convert_to_tensor(self._df)
        features = [tensor[index] for tensor in self._feature_tensor]
        if self._label_tensor is None:
            return (*features,)
        else:
            label = self._label_tensor[index]
            return (*features, label)

    def __getitem__(self, index):
        return self._get_next(index)

    def __len__(self):
        return self._size
