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

from typing import Any, Callable, Dict, Generic, List, NoReturn, Optional, Iterable, Iterator, Union

from raydp.parallel import _Dataset, _Shard
from raydp.parallel.interfaces import T, U

import pandas as pd

import ray
import random


def from_items(items: List[T],
               num_shards: int = 2,
               repeat: bool = False) -> "Dataset[T]":
    base_iterator = riter.from_items(items, num_shards, repeat)
    return Dataset(base_iterator)


def from_range(n: int,
               num_shards: int = 2,
               repeat: bool = False) -> "Dataset[int]":
    base_iterator = riter.from_range(n, num_shards, repeat)
    return Dataset(base_iterator)


def from_iterators(generators: List[Iterable[T]],
                   repeat: bool = False,
                   name=None) -> "RayDataset[T]":
    base_iterator = riter.from_iterators(generators, repeat, name)
    return Dataset(base_iterator)


def from_spark_df(df: "pyspark.sql.DataFrame",
                  num_shards: int = 2) -> "RayDataset[T]":
    return rcontext.save_to_ray(df, num_shards)


class BufferedIterator(Iterator[T]):
    def __init__(self, base_iterator: Iterator[T]):
        super(BufferedIterator, self).__init__()
        self._base_iterator: Iterator[T] = base_iterator
        self._head: T = None
        self._head_defined: bool = False

    def head(self) -> T:
        if not self._head_defined:
            self._head = self.__next__()
            self._head_defined = True
        return self._head

    def __iter__(self) -> Iterator[T]:
        return self

    def __next__(self) -> T:
        if self._head_defined:
            self._head_defined = False
            t = self._head
            self._head = None
            return t
        else:
            return self._base_iterator.__next__()


class IteratorShard(_Shard[T]):
    def __init__(self,
                 parent: Union[Iterable[T], "IteratorShard"[T]],
                 transform: Callable[[Iterable], Any] = None,
                 name: str = None,
                 shard_id: int = 0,
                 is_repeated: bool = False):
        self._parent = parent
        self._lazy_transform: Callable[[Iterable], Iterator] = transform or (lambda x: x)
        self._name: str = name or "unknown"
        self._shard_id: int = shard_id
        self._is_repeated: bool = is_repeated

        self._built_iterator: Iterator[T] = None

    def transform(self,
                  fn: Callable[[Iterable[T]], Iterator[U]],
                  fn_name: str = ".transform()",
                  is_repeated: bool = False) -> "IteratorShard[U]":
        """
        Transform the underlying iterator to another iterator. This is a lazy execute function.
        :param fn: the transform function
        :param fn_name: the function name
        :param is_repeated: whether the transform function will generate a repeated Iterator
        :return: a transformed new IteratorShard
        """
        return IteratorShard(self, fn, self._name + fn_name, self._shard_id, is_repeated)

    def build_iterator(self, ignore_existed: bool = False) -> Iterator[T]:
        """
        Build the iterator. This will create the first iterator and apply all lazy transform
        functions to the iterator.
        :param ignore_existed: whether create a new Iterator despite has built
        :return: the built iterator.
        """
        if ignore_existed or self._built_iterator is None:
            it = iter(self._parent)
            self._built_iterator = self._lazy_transform(it)
        return self._built_iterator

    def get_shard_id(self) -> int:
        return self._shard_id

    def is_repeated(self) -> bool:
        return self._is_repeated

    def barrier(self) -> bool:
        return True

    def __str__(self):
        return repr(self)

    def __repr__(self):
        return "IteratorShard[{}]".format(self.name)

    def __iter__(self):
        self.build_iterator()
        return self._built_iterator

    def __next__(self):
        self.build_iterator()
        return next(self._built_iterator)


class ShardExecutor:
    def __init__(self,
                 base_data_creator: Callable[[], Iterable[T]],
                 original_name: str = "unknown",
                 shard_id: int = 0,
                 original_is_repeated: bool = False):
        self._base_data_creator: Callable[[], Iterable[T]] = base_data_creator
        self._original_name = original_name
        self._shard_id = shard_id
        self._is_repeated = original_is_repeated
        self._shard: IteratorShard[T] = None

    def build_shard(self, transforms: List[Callable[[IteratorShard[T]], IteratorShard[U]]] = []):
        it = self._shard
        for fn in transforms:
            it = fn(it)
        self._shard = it

    def cache(self):
        if self._is_repeated:
            raise Exception("Can not cache for repeated iterator")

        object_ids: List[ray.ObjectRef] = []
        self._shard.build_iterator(True)
        for item in self._shard:
            object_ids.append(ray.put(item))

        def cache_fn(it) -> Iterator[T]:
            for object_id in object_ids:
                yield ray.get(object_id)
        self._shard = IteratorShard(
            iter([1]), cache_fn, str(self), self._shard_id, self._is_repeated)

    def repeat(self):
        if self._is_repeated:
            return
        self._is_repeated = True

        def repeat_fn(it: Iterable[T]) -> Iterator[T]:
            while True:
                for item in it:
                    yield item
        self._shard = IteratorShard(
            self._shard, repeat_fn, str(self), self._shard_id, self._is_repeated)

    def __str__(self):
        return repr(self)

    def __repr__(self):
        if self._shard is not None:
            name = str(self._shard)
        else:
            name = self._original_name

        return name


class Dataset(_Dataset[T]):
    def __init__(self,
                 shards: List[ray.actor.ActorHandle] = None,
                 base_data_creators: List[Callable[[], Iterable[T]]] = None,
                 shard_resources: Union[Dict[str, float], List[Dict[str, float]]] = None,
                 name: str = "unknown",
                 is_repeated: bool = False,
                 transforms: List[Callable[[Iterable[T]], Iterator[U]]] = None):
        if shards is not None:
            self._shards = shards
        else:
            assert base_data_creators is not None
            remote_cls = ray.remote(ShardExecutor)
            shard_resources = shard_resources or {}
            if isinstance(shard_resources, dict):
                shard_resources = [shard_resources] * self._num_shards

            shards = []
            assert len(base_data_creators) == len(shard_resources)
            for i, (creator, spec) in enumerate(zip(base_data_creators, shard_resources)):
                shards.append(remote_cls.options(**spec).remote(
                    creator, self._name, i, self._is_repeated))
            self._shards = shards

        self._base_data_creators = base_data_creators
        self._shard_resources = shard_resources
        self._name: str = name
        self._is_repeated: bool = is_repeated

        self._num_shards: int = len(base_data_creators)
        self._lazy_transform: List[Callable[[Iterable[T]], Iterator[U]]] = transforms or []

    def _trigger_transform(self):
        for shard in self._shards:
            shard.build_shard.remote(self._lazy_transform)
        self._lazy_transform = []

    def transform(self,
                  fn: Callable[[Iterable[T]], Iterator[U]],
                  fn_name: str) -> "Dataset[U]":
        return Dataset(self._shards,
                       self._base_data_creators,
                       self._shard_resources,
                       self._name + fn_name,
                       self._is_repeated,
                       self._lazy_transform + [fn])

    def map(self, fn: Callable[[T], U]) -> "Dataset[U]":
        """
        Map the given function to each item of all the shards iterators. This is a lazy execute
        function.
        :param fn: the map function
        :return: a new Dataset with map function applied
        """
        def map_fn(it: Iterable[T]) -> Iterator[U]:
            for item in it:
                yield fn(item)
        return self.transform(map_fn, ".map()")

    def filter(self, fn: Callable[[T], bool]) -> "Dataset[T]":
        """
        Filter the Dataset. This is a lazy execute function.
        :param fn: the filter function.
        :return: a filtered Dataset
        """
        def filter_fn(it: Iterable[T]) -> Iterator[T]:
            for item in it:
                if fn(item):
                    yield item
        return self.transform(filter_fn, ".filter()")

    def flatmap(self, fn: Callable[[T], List[U]]) -> "Dataset[U]":
        """
        Flatmap on all shard iterators. This is a lazy execute function.
        :param fn: the flatmap function
        :return: a new Dataset with flatmap function applied
        """
        def flatmap_fn(it: Iterable[T]) -> Iterator[U]:
            for item in it:
                for f_item in fn(item):
                    yield f_item
        return self.transform(flatmap_fn, ".flatmap()")

    def flatten(self) -> "Dataset[T[0]]":
        """
        Flatten the Iterator[Iterable[T]] to Iterator[T] on all shard iterators.
        This is a lazy execute function.
        :return: flattened Dataset
        """
        def flatten_fn(it: Iterable[T]) -> Iterator[T[0]]:
            for item in it:
                for subitem in item:
                    yield subitem

        return self.transform(flatten_fn, ".flatten()")

    def shuffle(self,
                shuffle_buffer_size: int,
                seed: int = None,
                global_shuffle: bool = False) -> "Dataset[T]":
        """
        Shuffle items of this Dataset. This is a lazy execute function.
        :param shuffle_buffer_size: The algorithm fills a buffer with shuffle_buffer_size elements
                                    and randomly samples elements from this buffer, replacing the
                                    selected elements with new elements. For perfect shuffling,
                                    this argument should be greater than or equal to the largest
                                    iterator size.
        :param seed: Seed to use for randomness. Default value is None.
        :param global_shuffle: whether shuffle on the global Dataset. Currently, we only support
                               shuffle on each shard locally.
        :return: A shuffled Dataset
        """

        if global_shuffle:
            raise Exception("global shuffle has not been supported now")

        shuffle_random = random.Random(seed)

        def shuffle_fn(it):
            buffer = []
            for item in it:
                buffer.append(item)
                if len(buffer) >= shuffle_buffer_size:
                    yield buffer.pop(shuffle_random.randint(0, len(buffer) - 1))

            while len(buffer) > 0:
                yield buffer.pop(shuffle_random.randint(0, len(buffer) - 1))

        name = f".shuffle(shuffle_buffer_size={shuffle_buffer_size}, seed={seed or None})"
        return self.transform(shuffle_fn, name)

    def batch(self, batch_size: int) -> "Dataset[List[T]]":
        """
        Batch the items into batch_size size. This is a lazy execute function.
        :param batch_size: the batch size
        :return: a batched Dataset
        """
        def batch_fn(it: Iterable[T]) -> Iterator[List[T]]:
            batch = []
            for item in it:
                batch.append(item)
                if len(batch) >= batch_size:
                    yield batch
                    batch = []
            if batch:
                yield batch

        return self.transform(batch_fn, f".batch({batch_size})")

    def cache(self) -> "Dataset[T]":
        """
        Cache each item into ray object store. The item should be a batch list for better
        performance. This is a lazy execute function.
        :return: a cached Dataset
        """
        if self._is_repeated:
            raise Exception("Can not cache for repeated iterator")

        self._trigger_transform()
        for shard in self._shards:
            shard.cache.remote()
        return Dataset(self._shards, self._base_data_creators, self._shard_resources,
                       self._name + ".cache()", False, [])

    def repeat(self):
        """
        Convert the iterator of each shard into a repeated iterator.
        :return: a repeated Dataset.
        """
        if self._is_repeated:
            return
        self._trigger_transform()
        for shard in self._shards:
            shard.repeat.remote()
        return Dataset(self._shards, self._base_data_creators, self._shard_resources,
                       self._name + ".repeat()", False, [])

    def take(self, n) -> List[T]:
        """
        Take the first n items. This is a eager execute function.
        """
        data = []
        for item in self:
            data.append(item)
            if len(data) >= n:
                break
        return data

    def show(self, n) -> NoReturn:
        """
        Print the first n items. This is a eager execute function.
        """
        self.assert_not_built("show")
        i = 0
        for item in self:
            print(item)
            i += 1
            if i >= n:
                break

    def count(self) -> int:
        """
        Return the elements count. This is a eager execute function.
        :return: the elements count
        """
        self.assert_not_built("count")
        it = self.build_iterator()
        return len(iter(it))

    def get_shard(self, index: int, **kwargs) -> IteratorShard[T]:
        pass

    def num_shards(self) -> int:
        return self._num_shards

    def to_pandas(self) -> "PandasDataset":
        pass


class PandasDataset(Dataset[pd.DataFrame]):
    """
    This is a special RayDataset that each element is a pandas.DataFrame, and also transform
    function should return a PandasDataset too.
    """
    def __iter__(self, base_data: Dataset[pd.DataFrame]):
        super(PandasDataset, self).__init__(base_data)

    def to_torch(self,
                 feature_columns: List[str] = None,
                 feature_types: Optional[List["torch.dtype"]] = None,
                 feature_shapes: Optional[List[Any]] = None,
                 label_column: str = None,
                 label_type: Optional["torch.dtype"] = None):
        """
        This will
        :param feature_columns:
        :param feature_shapes:
        :param feature_types:
        :param label_column:
        :param label_type:
        :return:
        """
        pass

    def to_torch(self,
                 feature_columns: List[str],
                 feature_types: List["tensorflow.DType"],
                 feature_shapes: List["tensorflow.TensorShape"],
                 label_column: str,
                 label_type: "tensorflow.DType",
                 label_shape: "tensorflow.TensorShape",
                 shuffle: bool):
        pass

