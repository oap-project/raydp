from collections.abc import Iterable
from typing import Any, List, Optional

import numpy as np
import pandas
import torch
from torch.utils.data import Dataset, DistributedSampler

from raydp.spark.context import save_to_ray
from raydp.spark.resource_manager.exchanger import SharedDataset
from raydp.spark.utils import BLOCK_SIZE_BIT, divide_blocks


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

        self._feature_tensor = None
        self._label_tensor = None

    def _check_and_convert(self):
        # convert to list for convenience
        if not isinstance(self._feature_columns, List):
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

        if not self._label_type:
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
            self._feature_tensor = tensors
        else:
            feature_columns = (self._feature_columns if
                               len(self._feature_columns) > 1 else self._feature_columns[0])
            feature_df = df[feature_columns].values
            t = torch.as_tensor(feature_df, dtype=self._feature_types[0])
            self._feature_tensor = [t]

        label_df = df[self._label_column].values
        self._label_tensor = torch.as_tensor(label_df, dtype=self._label_type)

    def _get_next(self, index):
        label = self._label_tensor[index]
        features = [tensor[index] for tensor in self._feature_tensor]
        return (*features, label)


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
        self._unresolved_shared_dataset: SharedDataset = None
        self._resolved_shared_dataset: SharedDataset = None
        self._previous_block_index = -1

        self._check_and_convert()

        if df is not None:
            self._unresolved_shared_dataset = save_to_ray(df)

    def _resolve_with_indices(self,
                              indices: List[int],
                              plasma_store_socket_name: Optional[str]):
        resolved_shared_dataset = self._unresolved_shared_dataset.subset(indices)
        resolved_shared_dataset.set_plasma_store_socket_name(plasma_store_socket_name)
        resolved_shared_dataset.resolve()
        self._resolved_shared_dataset = resolved_shared_dataset

    def __getitem__(self, index):
        block_index = index >> BLOCK_SIZE_BIT
        block_inner_index = (block_index << BLOCK_SIZE_BIT) ^ index
        if block_index != self._previous_block_index:
            self._previous_block_index = block_index
            df = self._resolved_shared_dataset[block_index]
            self._convert_to_tensor(df)
        return self._get_next(block_inner_index)

    def __len__(self):
        """Get the total size"""
        return self._unresolved_shared_dataset.total_size()

    def block_sizes(self) -> List[int]:
        """Get the block sizes"""
        return self._unresolved_shared_dataset.partition_sizes()

    @classmethod
    def _custom_deserialize(cls,
                            data_set: SharedDataset,
                            feature_columns: List[str],
                            feature_shapes: List[Any],
                            feature_types: List[torch.dtype],
                            label_column: str,
                            label_type: torch.dtype):
        instance = cls(
            None, feature_columns, feature_shapes, feature_types, label_column, label_type)
        instance._unresolved_shared_dataset = data_set
        return instance

    def __reduce__(self):
        return (RayDataset._custom_deserialize,
                (self._unresolved_shared_dataset, self._feature_columns, self._feature_shapes,
                 self._feature_types, self._label_column, self._label_type))


class BlockSetSampler(DistributedSampler):
    """
    A distributed sampler for BlockSet.

    We will shuffle the blocks order and then shuffle the block inner if shuffle is set to True.
    """
    def __init__(self, dataset, num_replicas=None, rank=None, shuffle=True, init_lazy=True):
        assert isinstance(dataset, RayDataset)
        self._args = (dataset, num_replicas, rank, shuffle)
        self._inited = False

        self._block_indices = None
        self._selected_indices = None

        if not init_lazy:
            self._init_lazy()

    def _init_lazy(self):
        """
        This is a workaround because of ray sgd call initialize the data creator before of
        setup distributed components.
        """
        if not self._inited:
            super(BlockSetSampler, self).__init__(*self._args)
            self._split_blocks()
            self._inited = True

    def _split_blocks(self):
        block_indexes, packed_selected_indexes = divide_blocks(
            self.dataset.block_sizes(), self.num_replicas, self.rank, self.shuffle)
        self._block_indices = block_indexes
        self._selected_indices = packed_selected_indexes

    def resolve(self, plasma_store_socket_name: Optional[str] = None):
        """Manually trigger the underlying object transfer."""
        self._init_lazy()
        self.dataset._resolve_with_indices(self._block_indices,
                                           plasma_store_socket_name)

    @property
    def block_indices(self):
        return self._block_indices

    def __iter__(self):
        self.resolve()
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
        # if we use `if sampler` to determine whether the sampler is None,
        # it will call this method. This can be happened when the BlockSetSampler
        # used in the evaluation in ray TorchTrainer.
        self._init_lazy()
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
        super(PandasDataset, self).__init__(feature_columns, feature_shapes,
                                            feature_types, label_column, label_type)
        self._check_and_convert()

        self._size = len(df)
        self._convert_to_tensor(df)

    def __getitem__(self, index):
        return self._get_next(index)

    def __len__(self):
        return self._size
