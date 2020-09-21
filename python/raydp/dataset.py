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

from typing import Any, Callable, List, Iterable, Generic, Optional, TypeVar

import ray.util.iter as ray_iter
from ray.util.iter import LocalIterator, ParallelIterator, SharedMetrics

import raydp.spark.context as context
from raydp.torch_dataset import IteratorDataset

try:
    import tensorflow as tf
except:
    pass

# The type of an iterator element.
T = TypeVar("T")
U = TypeVar("U")


class Shard(Generic[T]):
    def __init__(self, base_iter: LocalIterator[T]):
        self._base_iter = base_iter

    def base_iter(self) -> LocalIterator[T]:
        return self._base_iter

    def to_torch(self,
                 types: Optional[List["torch.dtype"]] = None,
                 shapes: Optional[List[Any]] = None) -> IteratorDataset:
        return IteratorDataset(self, types, shapes)

    def to_tf(self) -> "tensorflow.data.Dataset":
        pass

    def buffered(self) -> "Shard[T]":
        return Shard(BufferedLocalIterator(lambda: self._base_iter,
                                           self._base_iter.shared_metrics,
                                           self._base_iter.local_transforms,
                                           self._base_iter.timeout,
                                           self._base_iter.name + ".buffered()"))


class Dataset(Generic[T]):

    def __init__(self, base_data: ParallelIterator[T]):
        self._base_data: ParallelIterator[T] = base_data

    @classmethod
    def from_items(cls,
                   items: List[T],
                   num_shards: int = 2,
                   repeat: bool = False) -> "Dataset[T]":
        base_iterator = ray_iter.from_items(items, num_shards, repeat)
        return Dataset(base_iterator)

    @classmethod
    def from_range(cls,
                   n: int,
                   num_shards: int = 2,
                   repeat: bool = False) -> "Dataset[int]":
        base_iterator = ray_iter.from_range(n, num_shards, repeat)
        return Dataset(base_iterator)

    @classmethod
    def from_iterators(cls,
                       generators: List[Iterable[T]],
                       repeat: bool = False,
                       name=None) -> "Dataset[T]":
        base_iterator = ray_iter.from_iterators(generators, repeat, name)
        return Dataset(base_iterator)

    @classmethod
    def from_spark_df(cls, df: "pyspark.sql.DataFrame") -> "Dataset[T]":
        context.save_to_ray(df)

    def as_spark_df(self, spark: "pyspark.sql.SparkSession") -> "pyspark.sql.DataFrame":
        pass

    def transform(self, fn: Callable[[Iterable[T]], Iterable[U]]) -> "Dataset[U]":
        return Dataset(self._base_data.transform(fn))

    def map(self, fn: Callable[[T], U]) -> "Dataset[U]":
        return Dataset(self._base_data.for_each(fn))

    def flatmap(self, fn: Callable[[T], List[U]]) -> "Dataset[U]":
        return Dataset(self._base_data.combine(fn))

    def filter(self, fn: Callable[[T], T]) -> "Dataset[T]":
        return Dataset(self._base_data.filter(fn))

    def batch(self, batch_size: int) -> "Dataset[T]":
        return Dataset(self._base_data.batch(batch_size))

    def flatten(self) -> "Dataset[T[0]]":
        return Dataset(self._base_data.flatten())

    def shuffle(self,
                shuffle_buffer_size: int,
                seed: int = None,
                global_shuffle=False) -> "Dataset[T]":
        if global_shuffle:
            raise Exception("global_shuffle has not supported")
        return Dataset(self._base_data.local_shuffle(shuffle_buffer_size, seed))

    def repartition(self, num_partitions: int) -> "Dataset[T]":
        return Dataset(self._base_data.repartition(num_partitions))

    def take(self, n: int) -> List[T]:
        return self._base_data.take(n)

    def union(self, other: "Dataset[T]") -> "Dataset[T]":
        return Dataset(self._base_data.union(other._base_data))

    def num_shards(self) -> int:
        return self._base_data.num_shards()

    def get_shard(self, index: int) -> "Shard[T]":
        return Shard(self._base_data.get_shard(index))

    def gather_sync(self) -> "Shard[T]":
        return Shard(self._base_data.gather_sync())

    def gather_async(self, batch_ms=0, num_async=1) -> "Shard[T]":
        return Shard(self._base_data.gather_async(batch_ms, num_async))


class BufferedLocalIterator(LocalIterator[T]):
    def __init__(self,
                 base_iterator: Callable[[], Iterable[T]],
                 shared_metrics: SharedMetrics,
                 local_transforms: List[Callable[[Iterable], Any]] = None,
                 timeout: int = None,
                 name: str = None):
        super(BufferedLocalIterator, self).__init__(base_iterator,
                                                    shared_metrics,
                                                    local_transforms,
                                                    timeout, name)
        self._head: T = None
        self._head_defined: bool = False

    def head(self) -> T:
        if not self._head_defined:
            self._head = self.__next__()
            self._head_defined = True
        return self._head

    def __next__(self) -> T:
        if self._head_defined:
            self._head_defined = False
            t = self._head
            self._head = None
            return t
        else:
            return super().__next__()
