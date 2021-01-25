import functools
from typing import Any, List, Iterable, Optional

import numpy as np
import torch
from ray.util.data.dataset import MLDataset
from torch.utils.data.dataloader import DataLoader
from torch.utils.data.dataset import IterableDataset


def _convert_to_tensor(df, feature_columns: List[Any],
                       feature_shapes: List[Any],
                       feature_types: List[torch.dtype], label_column: Any,
                       label_shape: Optional[int], label_type: torch.dtype):
    feature_tensor = []
    for col, shape, dtype in zip(feature_columns, feature_shapes,
                                 feature_types):
        column = df[col].values
        if column.dtype == np.object:
            if isinstance(column[0], np.ndarray):
                column = np.stack(column)
            elif isinstance(column[0], (list, tuple)):
                column = list(column)
            else:
                raise Exception(
                    f"Column {col}'s type: {type(column[0])} is not supported."
                    " It must be numpy built in type or numpy object of "
                    "(ndarray, list, tuple)")

        t = torch.as_tensor(column, dtype=dtype)
        if shape is not None:
            t = t.view(*(-1, *shape))
        else:
            t = t.view(-1, 1)
        feature_tensor.append(t)

    label_df = df[label_column].values
    label_tensor = torch.as_tensor(label_df, dtype=label_type)
    if label_shape:
        label_tensor = label_tensor.view(-1, label_shape)
    else:
        label_tensor = label_tensor.view(-1, 1)
    return feature_tensor, label_tensor


def create_data_set(
        ds: MLDataset,
        num_partitions: int,
        feature_columns: List[str] = None,
        feature_shapes: Optional[List[Any]] = None,
        feature_types: Optional[List[torch.dtype]] = None,
        label_column: str = None,
        label_shape: Optional[int] = None,
        label_type: Optional[torch.dtype] = None):

    # convert to list for convenience
    if not isinstance(feature_columns, list):
        feature_columns = [feature_columns]

    if feature_shapes:
        if not isinstance(feature_shapes, list):
            feature_shapes = [feature_shapes]

        assert len(feature_columns) == len(feature_shapes), \
            "The feature_shapes size must match the feature_columns"
        for i in range(len(feature_shapes)):
            if not isinstance(feature_shapes[i], Iterable):
                feature_shapes[i] = [feature_shapes[i]]
    else:
        feature_shapes = [None] * len(feature_columns)

    if feature_types:
        if not isinstance(feature_types, list):
            feature_types = [feature_types]

        assert len(feature_columns) == len(feature_types), \
            "The feature_types size must match the feature_columns"
        for i in range(len(feature_types)):
            assert (all(isinstance(dtype, torch.dtype)
                        for dtype in feature_types)), \
                "All value in feature_types should be torch.dtype instance"
    else:
        feature_types = [torch.float] * len(feature_columns)

    if not label_type:
        label_type = torch.float

    convert_fn = functools.partial(
        _convert_to_tensor,
        feature_columns=feature_columns,
        feature_shapes=feature_shapes,
        feature_types=feature_types,
        label_column=label_column,
        label_shape=label_shape,
        label_type=label_type)
    torch_dataset = TorchDataset(ds, num_partitions, convert_fn)
    return torch_dataset


class TorchDataset(IterableDataset):
    def __init__(self, ds: MLDataset, num_shards, convert_fn):
        assert ds.num_shards() >= num_shards
        self._ds = ds
        self._num_shards = num_shards
        self._convert_fn = convert_fn

    def __iter__(self):
        it = self._ds.gather_async(batch_ms=0, num_async=self._ds.num_shards())
        it = it.for_each(self._convert_fn)
        return it

    def get_shard(self, shard_index: int) -> "TorchDataset":
        assert shard_index < self._num_shards
        shard_ids = []
        i = shard_index
        step = self._num_shards
        while i <= self._ds.num_shards():
            shard_ids.append(i)
            i += step
        ds = self._ds.select_shards(shard_ids)
        return TorchDataset(ds, ds.num_shards(), self._convert_fn)


class TorchDataLoader(DataLoader):
    def __init__(self, dataset, batch_size=128, shuffle=False, seed: int = None,
                 pin_memory=False):
        assert isinstance(dataset, TorchDataset)
        self.dataset = dataset
        self.batch_size = batch_size
        self.shuffle = shuffle
        self.seed = seed or 0
        self.epoch = 0
        self.pin_memory = pin_memory and torch.cuda.is_available()

    def __iter__(self):
        it = iter(self.dataset)
        torch.manual_seed(self.epoch + self.seed)
        return_ts = None
        while True:
            try:
                cur_ts = next(it)
                # shuffle the tensor
                cur_ts = cur_ts[torch.randperm(cur_ts.shape[0])]

                # rebatch the tensor to the given batch_size
                cur_index = 0
                cur_size = cur_ts.shape[0]
                while cur_ts is not None or (
                        cur_index + self.batch_size) < cur_size:
                    if cur_ts is None or cur_index == cur_size:
                        cur_ts = next(it)
                        cur_index = 0
                        cur_size = cur_ts.shape[0]
                    if return_ts is not None:
                        ri = cur_index + self.batch_size - return_ts.shape[0]
                        ri = min(ri, cur_size)
                        tmp = cur_ts[cur_index: ri]
                        return_ts = torch.cat([return_ts, tmp], dim=0)
                        cur_index = ri
                    else:
                        ri = cur_index + self.batch_size
                        ri = min(ri, cur_size)
                        return_ts = cur_ts[cur_index:ri]
                        cur_index = ri
                    if return_ts.shape[0] == self.batch_size:
                        if self.pin_memory:
                            return_ts =\
                                torch.utils.data._utils.pin_memory.pin_memory(return_ts)
                        yield return_ts
                        return_ts = None
            except StopIteration:
                break

        if return_ts is not None:
            if self.pin_memory:
                return_ts = torch.utils.data._utils.pin_memory.pin_memory(return_ts)
            yield return_ts
