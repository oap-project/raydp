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

import random
from typing import Any, Callable, Dict, List, NoReturn, Optional, Iterable, Iterator, Union

import pandas as pd
import ray

import raydp.context as rcontext
from raydp.parallel import _Dataset, _Shard
from raydp.parallel.interfaces import T, U


def from_items(items: List[T],
               num_shards: int = 2,
               repeat: bool = False) -> "Dataset[T]":
    shards = [[] for _ in range(num_shards)]
    for i, item in enumerate(items):
        shards[i % num_shards].append(item)
    name = "from_items[{}, {}, shards={}{}]".format(
        items and type(items[0]).__name__ or "None",
        len(items),
        num_shards,
        ", repeat=True" if repeat else "")
    return from_iterators(shards, repeat=repeat, name=name)


def from_range(n: int,
               num_shards: int = 2,
               repeat: bool = False) -> "Dataset[int]":
    generators = []
    shard_size = n // num_shards
    for i in range(num_shards):
        start = i * shard_size
        if i == num_shards - 1:
            end = n
        else:
            end = (i + 1) * shard_size
        generators.append(range(start, end))
    name = f"from_range[{n}, shards={num_shards} {', repeat=True' if repeat else ''}]"
    return from_iterators(generators, repeat=repeat, name=name)


def from_iterators(generators: List[Iterable[T]],
                   shard_resources: Union[Dict[str, float], List[Dict[str, float]]] = None,
                   repeat: bool = False,
                   name: str = None) -> "Dataset[T]":
    creators = []
    for gen in generators:
        if repeat:
            def base_generator() -> Iterator[T]:
                while True:
                    for item in gen:
                        yield item
        else:
            def base_generator() -> Iterator[T]:
                for item in gen:
                    yield item

        creators.append(base_generator)

    return Dataset(None, creators, shard_resources, name, repeat, None)


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
                 parent_or_generator: Union[Callable[[], Iterable[T]], "IteratorShard"[T]],
                 transforms: List[Callable[[Iterable], Any]] = None,
                 name: str = None,
                 shard_id: int = 0,
                 is_repeated: bool = False):
        self._parent_or_generator = parent_or_generator
        self._lazy_transforms: List[Callable[[Iterable], Iterator]] = transforms or []
        self._name: str = name or "unknown"
        self._shard_id: int = shard_id
        self._is_repeated: bool = is_repeated

    def transform(self,
                  fns: List[Callable[[Iterable[T]], Iterator[U]]],
                  fn_name: str = ".transform()",
                  is_repeated: bool = False) -> "IteratorShard[U]":
        """
        Transform the underlying iterator to another iterator. This is a lazy execute function.
        :param fns: the transform function
        :param fn_name: the function name
        :param is_repeated: whether the transform function will generate a repeated Iterator
        :return: a transformed new IteratorShard
        """
        return IteratorShard(self, fns, self._name + fn_name, self._shard_id, is_repeated)

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
        if callable(self._parent_or_generator):
            it = self._parent_or_generator()
        else:
            it = iter(self._parent_or_generator)
        if self._lazy_transforms is not None:
            for transform in self._lazy_transforms:
                it = transform(it)
        return it


class ShardExecutor:
    def __init__(self,
                 base_data_creator: Callable[[], Iterable[T]],
                 shard_name: str = "shard_executor",
                 shard_id: int = 0,
                 original_is_repeated: bool = False,
                 data_age: int = 0):
        """
        A actor that serves for the shard execution.
        :param base_data_creator: the base data or source data creator
        :param shard_name: the shard name
        :param shard_id: the shard id
        :param original_is_repeated: whether the original data that are generated by
                                     base_data_creator is repeated.
        :param data_age: the data age, each transform will increase the data age. It used
                         to identify whether the shard iterator has changed
        """
        self._shard: IteratorShard[T] = IteratorShard(
            base_data_creator, None, "source_generator", shard_id, original_is_repeated)
        self._shard_name = shard_name
        self._shard_id = shard_id
        self._is_repeated = original_is_repeated

        self._data_age = data_age
        self._built_it: Iterable[T] = None

    def get_data_age(self) -> int:
        return self._data_age

    def apply_transforms(self,
                         transforms: List[Callable[[Iterable[T]], Iterator[U]]] = [],
                         name: str = ".transform()"):
        # increase the data age by length of transforms
        self._data_age += len(transforms)
        self._shard = self._shard.transform(transforms, name, self._is_repeated)

    def cache(self):
        if self._is_repeated:
            raise Exception("Can not cache for repeated iterator")

        # cache will not change the data, so we don't need to increase the data age
        object_ids: List[ray.ObjectRef] = []
        it = iter(self._shard)
        for item in it:
            # cache each item into ray object store
            object_ids.append(ray.put(item))

        def restore_fn() -> Iterator[T]:
            # restore function
            for object_id in object_ids:
                yield ray.get(object_id)
        self._shard = IteratorShard(restore_fn, [], str(self._shard) + ".cache()",
                                    self._shard_id, self._is_repeated)

    def repeat(self):
        if self._is_repeated:
            return
        self._data_age += 1
        self._is_repeated = True

        def repeat_fn(it: Iterable[T]) -> Iterator[T]:
            while True:
                for item in it:
                    yield item
        self._shard = self._shard.transform([repeat_fn], ".repeat()", True)

    def get_batch(self, build_new: bool, batch_size: int) -> List[T]:
        if build_new:
            self._built_it = iter(self._shard)
        data = []
        for i in range(batch_size):
            try:
                data.append(next(self._built_it))
            except StopIteration:
                break
        return data

    def apply(self, build_new: bool, fn: Callable[[Iterable[T]], Any]) -> Any:
        if build_new:
            self._built_it = iter(self._shard)
        result = fn(self._built_it)
        return result

    def __str__(self):
        return repr(self)

    def __repr__(self):
        name = f"{self._shard_name}-{self._shard_id}"
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
        self._fn_name_path = ""

    def _trigger_transform(self):
        if len(self._lazy_transform) > 0:
            for shard in self._shards:
                shard.apply_transforms.remote(self._lazy_transform, self._fn_name_path)
            self._lazy_transform = []
            self._fn_name_path = ""

    def transform(self,
                  fn: Callable[[Iterable[T]], Iterator[U]],
                  fn_name: str) -> "Dataset[U]":
        self._fn_name_path = self._fn_name_path + fn_name
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
        performance. This is a eager execute function.
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
        :return return as a list, the length of results can be less than n
        """
        self._trigger_transform()
        valid_shards = list(self._shards)
        result = []

        def fetch(build_new: bool):
            to_remove_indexes = []
            for i, shard in enumerate(valid_shards):
                data = ray.get(shard.get_batch(build_new, 1).remote())
                if data is []:
                    to_remove_indexes.append(i)
                else:
                    result.append(data[0])
                    if len(result) == n:
                        break

            to_remove_indexes.reverse()
            for i in to_remove_indexes:
                valid_shards.pop(i)

        fetch(True)
        while len(result) < n and valid_shards is not None:
            fetch(False)

        return result

    def show(self, n) -> NoReturn:
        """
        Print the first n items. This is a eager execute function.
        """
        self._trigger_transform()
        data = self.take(n)
        i = 0
        for item in data:
            print(item)
            i += 1

    def count(self) -> int:
        """
        Return the elements count. This is a eager execute function.
        :return: the elements count
        """
        self._trigger_transform()
        lens = ray.get([shard.apply(True, lambda it: len(it)).remote() for shard in self._shards])
        return sum(lens)

    def collect(self, batch_size: int = 1) -> IteratorShard[T]:
        """
        Collect all data as a local IteratorShard.
        :param batch_size read batch_size of data from each shard once
        """
        self._trigger_transform()
        shards = []
        for shard_id in range(self._num_shards):
            shards.append(iter(self.get_shard(shard_id, batch_size)))

        def shard_creator():
            valid_shards = list(shards)
            to_remove_indexes = []

            while valid_shards:
                for i, shard in enumerate(valid_shards):
                    try:
                        yield next(shard)
                    except StopIteration:
                        to_remove_indexes.append(i)
                if len(to_remove_indexes) > 0:
                    to_remove_indexes.reverse()
                    for i in to_remove_indexes:
                        valid_shards.pop(i)
                    to_remove_indexes = []
        return IteratorShard(shard_creator, [], "local_shard(collect)")

    def get_shard(self, shard_id: int, batch_size: int = 1) -> IteratorShard[T]:
        """
        Get the given shard_id shard into a local IteratorShard.
        :param shard_id: the shard id
        :param batch_size: read batch_size of data from shard in once
        :return: a local IteratorShard
        """
        assert batch_size > 0, f"batch_size should be greater than zero, but got {batch_size}"
        assert shard_id < self._num_shards,\
            f"out of range(0, {self._num_shards}): shard_id({shard_id})"
        self._trigger_transform()
        shard = self._shards[shard_id]
        expected_shard_age = ray.get(shard.get_data_age.remote())

        def shard_generator():
            shard_age = ray.get(shard.get_data_age.remote())
            if shard_age != expected_shard_age:
                # we should check the shard age here, because we can call this generator multiple
                # times
                raise Exception(f"the shard({shard_id}) age has changed, now({shard_age}), "
                                f"expected({expected_shard_age}), you should get the new shard "
                                f"iterator")
            data = ray.get(shard.get_batch(True, batch_size).remote())
            if data is []:
                return
            for item in data:
                yield item
            while len(data) > 0:
                data = ray.get(shard.get_batch(False, batch_size).remote())
                if data is []:
                    return
                for item in data:
                    yield item
        return IteratorShard(shard_generator, [], f"local_shard({shard_id})")

    def num_shards(self) -> int:
        return self._num_shards

    def to_pandas(self, fn: Callable[[Iterable[T]], Iterator[pd.DataFrame]]) -> "PandasDataset":
        self.transform(fn, ".to_pandas()")
        return PandasDataset(self)


class PandasDataset(Dataset[pd.DataFrame]):
    """
    This is a special RayDataset that each element is a pandas.DataFrame, and all transform
    function should return a PandasDataset too.
    """
    def __iter__(self, base_data: Dataset[pd.DataFrame]):
        super(PandasDataset, self).__init__(base_data)

    def to_torch(self,
                 shard_id: Optional[int] = None,
                 feature_columns: List[str] = None,
                 feature_types: Optional[List["torch.dtype"]] = None,
                 feature_shapes: Optional[List[Any]] = None,
                 label_column: str = None,
                 label_type: Optional["torch.dtype"] = None):
        """
        This will create a torch dataset.
        :param shard_id create the torch dataset for the given shard_id data if it is not None,
                        else will create the dataset for all shards data.
        :param feature_columns: the feature columns' name
        :param feature_shapes: the expected feature shapes
        :param feature_types: the expected feature types
        :param label_column: the label column name
        :param label_type: the expected label type
        :return: a torch dataset
        """
        if shard_id is None:
            source_it = iter(self.collect())
        else:
            source_it = iter(self.get_shard(shard_id))


    def to_tf(self,
              shard_id: Optional[int] = None,
              feature_columns: List[str],
              feature_types: List["tensorflow.DType"],
              feature_shapes: List["tensorflow.TensorShape"],
              label_column: str,
              label_type: "tensorflow.DType",
              label_shape: "tensorflow.TensorShape"):
        pass

