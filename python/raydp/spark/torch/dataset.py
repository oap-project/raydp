from collections.abc import Iterable
from typing import Any, List, Optional

import math
import numpy as np
import pandas
import torch
from torch.utils.data import Dataset, DistributedSampler

from raydp.spark.block_holder import BlockSet
from raydp.spark.spark_cluster import save_to_ray

# we use 4 bytes for block size, this means each block can contain
# 4294967296 records
BLOCK_SIZE_BIT = 32


class _Dataset(Dataset):
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
                assert all(isinstance(dtype, torch.dtype) for dtype in self._feature_types), \
                    "All value in feature_types should be torch.dtype instance"

        if not self._feature_shapes and self._feature_types:
            assert all(dtype == self._feature_types[0] for dtype in self._feature_types), \
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
                t = torch.as_tensor(current_feature[i], dtype=dtype)
                if shape != [0]:
                    t = t.view(*shape)
                feature_tensors.append(t)
            return (*feature_tensors, label)
        else:
            feature = torch.as_tensor(current_feature, dtype=self._feature_types[0])
            return feature, label


class RayDataset(_Dataset):
    """
    Store Spark DataFrame or koalas.DataFrame into ray object store and wrap into a torch
    Dataset which could be used by torch DataLoader.
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
        super(RayDataset, self).__init__(feature_columns, feature_shapes,
                                         feature_types, label_column, label_type)
        self._block_set: BlockSet = None
        self._previous_block_index = 0
        self._feature_df = None
        self._label_df = None

        self._check()

        if df is not None:
            self._block_set = save_to_ray(df)

    def _resolve_with_indices(self, indices: List[int]):
        self._block_set.resolve(indices, True)

    def __getitem__(self, index):
        worker_info = torch.utils.data.get_worker_info()
        if worker_info:
            # TODO: add support
            raise Exception("Multiple processes loading is not supported")

        global BLOCK_SIZE_BIT
        block_index = index >> BLOCK_SIZE_BIT
        block_inner_index = (block_index << BLOCK_SIZE_BIT) ^ index
        if block_index != self._previous_block_index:
            self._previous_block_index = block_index
            df = self._block_set[block_index]
            self._feature_df = df[self._feature_columns].values
            self._label_df = df[self._label_column].values
        return self._get_next(block_inner_index, self._feature_df, self._label_df)

    def __len__(self):
        """Get the total size"""
        return self._block_set.total_size

    def block_sizes(self) -> List[int]:
        """Get the block sizes"""
        return self._block_set.block_sizes

    @classmethod
    def _custom_deserialize(cls,
                            block_set: BlockSet,
                            feature_columns: List[str],
                            feature_shapes: List[Any],
                            feature_types: List[torch.dtype],
                            label_column: str,
                            label_type: torch.dtype):
        instance = cls(
            None, feature_columns, feature_shapes, feature_types, label_column, label_type)
        instance._block_set = block_set
        return instance

    def __reduce__(self):
        return (RayDataset._custom_deserialize,
                (self._block_set, self._feature_columns, self._feature_shapes, self._feature_types,
                 self._label_column, self._label_type))


class BlockSetSampler(DistributedSampler):
    """
    A distributed sampler for BlockSet.

    We will shuffle the blocks order and then shuffle the block inner if shuffle is set to True.
    """
    def __init__(self, dataset, num_replicas=None, rank=None, shuffle=True):
        assert isinstance(dataset, RayDataset)
        super(BlockSetSampler, self).__init__(dataset, num_replicas, rank, shuffle)

        self._block_indices = None
        self._selected_indices = None
        self._split_blocks()

    def _split_blocks(self):
        num_blocks = int(math.ceil(len(self.dataset.block_sizes()) * 1.0 / self.num_replicas))
        total_block_size = num_blocks * self.num_replicas
        g = torch.Generator()
        g.manual_seed(0)
        if self.shuffle:
            total_indices = torch.randperm(len(self.dataset.block_sizes()), generator=g).tolist()
        else:
            total_indices = list(range(len(self.dataset.block_sizes())))
        # add extra samples to make it evenly divisible
        total_indices += total_indices[:(total_block_size - len(total_indices))]
        assert len(total_indices) == total_block_size

        indices = total_indices[self.rank: total_block_size: self.num_replicas]
        assert len(indices) == num_blocks

        def select(i, current_size, selected) -> int:
            block_size = self.dataset.block_sizes()[i]
            tmp = current_size + block_size
            if tmp < self.num_samples:
                selected.append((i, block_size))
                current_size = tmp
            elif tmp >= self.num_samples:
                selected.append((i, (self.num_samples - current_size)))
                current_size = self.num_samples
            return current_size

        total_size = 0
        selected_indices = []
        for i in indices:
            total_size = select(i, total_size, selected_indices)
            if total_size == self.num_samples:
                break

        step = 1
        while total_size < self.num_samples:
            index = total_indices[(self.rank + step) % len(total_indices)]
            total_size = select(index, total_size, selected_indices)
            step += self.num_replicas

        assert total_size == self.num_samples

        block_indices, selected_size = list(zip(*selected_indices))
        self._block_indices = list(block_indices)

        indices = []
        global BLOCK_SIZE_BIT
        for index, size in selected_indices:
            # we use 4 Bytes for the block inner index
            indices.append([((index << BLOCK_SIZE_BIT) | i) for i in range(size)])
        self._selected_indices = indices

    @property
    def block_indices(self):
        return self._block_indices

    def __iter__(self):
        self.dataset._resolve_with_indices(self._block_indices)
        # deterministically shuffle based on epoch
        np.random.seed(self.epoch)
        block_indices = list(range(len(self._block_indices)))
        if self.shuffle:
            np.random.shuffle(block_indices)

        indices = []
        for index in block_indices:
            tmp = self._selected_indices[index]
            tmp = np.copy(tmp)
            if self.shuffle:
                np.random.shuffle(tmp)
            indices += tmp.tolist()

        return iter(indices)

    def __len__(self):
        return self.num_samples


class PandasDataset(_Dataset):
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
