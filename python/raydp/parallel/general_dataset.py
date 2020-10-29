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
from .interfaces import _Dataset, _Shard, T, U
from .sources import CacheDataSource, GeneratorSource, SourceShard


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
                   repeatable: bool = True,
                   repeat: bool = False,
                   name: str = None) -> "Dataset[T]":
    creators = []
    if repeat:
        repeatable = False
    for i, gen in enumerate(generators):
        if repeat:
            def make_generator(gen):
                def base_generator() -> Iterator[T]:
                    while True:
                        for item in gen:
                            yield item
                return base_generator
        else:
            def make_generator(gen):
                def base_generator() -> Iterator[T]:
                    for item in gen:
                        yield item
                return base_generator
        source_shard = GeneratorSource(name + str(i), make_generator(gen), repeatable, repeat)
        creators.append(source_shard)

    return Dataset(creators, shard_resources, name, repeatable, repeat, None)


def from_spark_df(df: "pyspark.sql.DataFrame",
                  num_shards: int = 2) -> "Dataset[T]":
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
                 parent: _Shard[T],
                 transforms: List[Callable[[Iterable], Any]] = None,
                 name: str = None,
                 shard_id: int = 0,
                 is_repeatable: bool = False,
                 is_repeated: bool = False):
        self._parent = parent
        self._lazy_transforms: List[Callable[[Iterable], Iterator]] = transforms or []
        self._shard_id: int = shard_id
        self._name: str = name or f"IteratorShard(id: {shard_id})"
        self._is_repeatable: bool = is_repeatable
        self._is_repeated: bool = is_repeated

        self._size: int = -1

    def transform(self,
                  fns: List[Callable[[Iterable[T]], Iterator[U]]],
                  fn_name: str = ".transform()",
                  is_repeatable: bool = True,
                  is_repeated: bool = False) -> "IteratorShard[U]":
        """
        Transform the underlying iterator to another iterator. This is a lazy execute function.
        :param fns: the transform function
        :param fn_name: the function name
        :param is_repeatable whether the IteratorShard is repeatable after transform
        :param is_repeated whether the IteratorShard is repeated after transform
        :return: a transformed new IteratorShard
        """
        return IteratorShard(self, fns, self._name + fn_name, self._shard_id,
                             is_repeatable, is_repeated)

    def repeat(self) -> "IteratorShard[T]":
        if self.repeated():
            # TODO: maybe fire a warning to user
            return self

        if not self.repeatable():
            raise Exception("can not repeat on an unrepeatable shard")

        return IteratorShard(self._parent, self._lazy_transforms, self._name + ".repeat()",
                             self._shard_id, False, True)

    def cache(self) -> "IteratorShard[T]":
        if self._is_repeated:
            raise Exception("Can not cache for repeated shard")

        # cache will not change the data, so we don't need to increase the data age
        object_ids: List[ray.ObjectRef] = []
        it = iter(self)
        for item in it:
            # cache each item into ray object store
            object_ids.append(ray.put(item))

        def restore_fn() -> Iterator[T]:
            # restore function
            for object_id in object_ids:
                yield ray.get(object_id)
        shard = CacheDataSource(object_ids, restore_fn, str(self) + ".cache()",
                                True, False)
        return IteratorShard(
            shard, [], shard.name(), self._shard_id, shard.repeatable(), shard.repeated())

    def get_shard_id(self) -> int:
        return self._shard_id

    def repeatable(self) -> bool:
        return self._is_repeatable

    def name(self) -> str:
        return self._name

    def repeated(self) -> bool:
        return self._is_repeated

    def __repr__(self):
        return "IteratorShard[{}]".format(self._name)

    def __iter__(self):
        # we need to repeat the iter only when parent is not a repeated iterator and self is
        repeated = self._is_repeated and not self._parent.repeated()
        if repeated:
            while True:
                it = iter(self._parent)
                for transform in self._lazy_transforms:
                    it = transform(it)
                for item in it:
                    yield item
        else:
            it = iter(self._parent)
            for transform in self._lazy_transforms:
                it = transform(it)
            for item in it:
                yield item

    def __len__(self):
        if self._is_repeated:
            raise Exception("IteratorShard is repeated")
        if self._size != -1:
            return self._size

        self._size = sum([1 for i in iter(self)])
        return self._size


class ShardExecutor:
    def __init__(self,
                 shard_name: str = "shard_executor",
                 shard_id: int = 0):
        """
        A actor that serves for the shard execution.
        :param shard_name: the shard name
        :param shard_id: the shard id
        """
        self._shard: IteratorShard[T] = None
        self._shard_name = shard_name
        self._shard_id = shard_id
        self._data_age: int = 0
        self._built_it: Iterable[T] = None

    def get_data_age(self) -> int:
        return self._data_age

    def execute(self,
                source_data: SourceShard[T],
                transforms: List[Callable[[IteratorShard[T]], IteratorShard[U]]] = []):
        self._data_age += 1
        shard = IteratorShard(source_data, [], source_data.name(), self._shard_id,
                              source_data.repeatable(), source_data.repeated())
        for transform in transforms:
            shard = transform(shard)
        self._shard = shard

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
                 source_data: List[SourceShard[T]] = None,
                 shard_resources: Union[Dict[str, float], List[Dict[str, float]]] = None,
                 name: str = "unknown",
                 repeatable: bool = True,
                 is_repeated: bool = False,
                 transforms: List[Callable[[IteratorShard[T]], IteratorShard[U]]] = None,
                 shards: List[ray.actor.ActorHandle] = None,):
        if shards is not None:
            self._shards = shards
            self._num_shards = len(shards)
        else:
            assert source_data is not None
            self._shards = None
            self._num_shards = len(source_data)

        self._source_data = source_data
        shard_resources = shard_resources or {}
        if isinstance(shard_resources, dict):
            shard_resources = [shard_resources] * len(source_data)
        self._shard_resources = shard_resources

        self._name: str = name
        self._repeatable: bool = repeatable
        self._is_repeated: bool = is_repeated

        self._lazy_transform = transforms or []

    def _trigger_transform(self, shard_id: Optional[int] = None):
        if self._shards is None:
            remote_cls = ray.remote(ShardExecutor)

            shards = []
            for i, spec in enumerate(self._shard_resources):
                shards.append(remote_cls.options(**spec).remote(self._name, i))
            self._shards = shards

        if shard_id is not None:
            self._shards[shard_id].execute.remote(
                self._source_data[shard_id], self._lazy_transform)
        else:
            for i, shard in enumerate(self._shards):
                shard.execute.remote(self._source_data[i], self._lazy_transform)

    def _transform(self,
                   fn: Callable[[IteratorShard[T]], IteratorShard[U]],
                   fn_name: str,
                   repeatable: bool,
                   is_repeated: bool) -> "Dataset[U]":

        return Dataset(
            self._source_data,
            self._shard_resources,
            self._name + fn_name,
            repeatable,
            is_repeated,
            self._lazy_transform + [fn],
            self._shards)

    def transform(self,
                  fn: Callable[[Iterable[T]], Iterator[U]],
                  fn_name: str,
                  repeatable: bool = True,
                  is_repeated: bool = False) -> "Dataset[U]":
        def transform_fn(it: IteratorShard[T]) -> IteratorShard[U]:
            return it.transform([fn], fn_name)
        return self._transform(transform_fn, fn_name, repeatable, is_repeated)

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
        return self.transform(map_fn, ".map()", self._repeatable, self._is_repeated)

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
        return self.transform(filter_fn, ".filter()", self._repeatable, self._is_repeated)

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
        return self.transform(flatmap_fn, ".flatmap()", self._repeatable, self._is_repeated)

    def flatten(self) -> "Dataset[U]":
        """
        Flatten the Iterator[Iterable[T]] to Iterator[T] on all shard iterators.
        This is a lazy execute function.
        :return: flattened Dataset
        """
        def flatten_fn(it: Iterable[T]) -> Iterator[U]:
            for item in it:
                for subitem in item:
                    yield subitem

        return self.transform(flatten_fn, ".flatten()", self._repeatable, self._is_repeated)

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
        return self.transform(shuffle_fn, name, self._repeatable, self._is_repeated)

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

        return self.transform(
            batch_fn, f".batch({batch_size})", self._repeatable, self._is_repeated)

    def cache(self) -> "Dataset[T]":
        """
        Cache each item into ray object store. The item should be a batch list for better
        performance. This is a lazy execute function.
        :return: a cached Dataset
        """
        if self._is_repeated:
            raise Exception("Can not cache for repeated Dataset")

        def cache_fn(it: IteratorShard[T]) -> IteratorShard[U]:
            return it.cache()
        return self._transform(cache_fn, ".cache()", self._repeatable, False)

    def name(self):
        return self._name

    def repeated(self) -> bool:
        return self._is_repeated

    def repeatable(self) -> bool:
        return self._repeatable

    def repeat(self) -> "Dataset[T]":
        if self._is_repeated:
            return self

        def repeat_fn(it: IteratorShard[T]) -> IteratorShard[T]:
            return it.repeat()
        return self._transform(repeat_fn, ".repeat()", False, True)

    def take(self, n) -> List[T]:
        """
        Take the first n items. This is a eager execute function.
        :return return as a list, the length of results can be less than n
        """
        assert n >= 0
        if n == 0:
            return []
        self._trigger_transform()
        valid_shards = list(self._shards)
        result = []

        def fetch(build_new: bool):
            to_remove_indexes = []
            for i, shard in enumerate(valid_shards):
                data = ray.get(shard.get_batch.remote(build_new, 1))
                if not data:
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
        if self._is_repeated:
            raise Exception("Can not count for repeated Dataset")
        self._trigger_transform()

        def count_fn(it):
            counts = [1 for i in it]
            return sum(counts)

        lens = ray.get([shard.apply.remote(True, count_fn) for shard in self._shards])
        return sum(lens)

    def collect(self, batch_size: int = 1) -> IteratorShard[T]:
        """
        Collect all data as a local IteratorShard.
        :param batch_size read batch_size of data from each shard once
        """

        def shard_creator():
            shards = []
            for shard_id in range(self._num_shards):
                shards.append(iter(self.get_shard(shard_id, batch_size)))
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
        name = f"local_shard(collect)"
        source = GeneratorSource(name, shard_creator, False, False)
        return IteratorShard(
            source, [], source.name(), 0, source.repeatable(), source.repeated())

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
        self._trigger_transform(shard_id)
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
            data = ray.get(shard.get_batch.remote(True, batch_size))
            if not data:
                return
            for item in data:
                yield item
            while len(data) > 0:
                data = ray.get(shard.get_batch.remote(False, batch_size))
                if not data:
                    return
                for item in data:
                    yield item
        name = f"local_shard({shard_id})"
        source = GeneratorSource(name, shard_generator, True, False)
        return IteratorShard(
            source, [], source.name(), shard_id, source.repeatable(), source.repeated())

    def apply(self, fn: Callable[[Iterable[T]], U]) -> List[U]:
        """
        This is a eager function. Apply the given fn to all the shards.
        :param fn: the function
        :return: a list of those shard results
        """
        self._trigger_transform()
        results = ray.get([shard.apply.remote(True, fn) for shard in self._shards])
        return results

    def num_shards(self) -> int:
        return self._num_shards

    def to_pandas(self,
                  fn: Optional[Callable[[Iterable[T]], Iterator[pd.DataFrame]]]
                  ) -> "PandasDataset":
        if fn is not None:
            ds = self.transform(fn, ".to_pandas()")
        else:
            ds = self
        return PandasDataset(ds)


class PandasDataset(Dataset[pd.DataFrame]):
    """
    This is a special RayDataset that each element is a pandas.DataFrame, and all transform
    function should return a PandasDataset too.
    """
    def __init__(self, base_data: Dataset[pd.DataFrame]):
        super(PandasDataset, self).__init__(base_data._source_data,
                                            base_data._shard_resources,
                                            base_data.name(),
                                            base_data.repeatable(),
                                            base_data.repeated(),
                                            base_data._shards)

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
        from raydp.torch import TorchIterablePandasDataset
        if shard_id is None:
            it = self.collect()
        else:
            it = self.get_shard(shard_id)
        return TorchIterablePandasDataset(it=it,
                                          feature_columns=feature_columns,
                                          feature_types=feature_types,
                                          feature_shapes=feature_shapes,
                                          label_column=label_column,
                                          label_type=label_type)

    def to_tf(self,
              shard_id: Optional[int] = None,
              feature_columns: List[str] = None,
              feature_types: List["tensorflow.DType"] = None,
              feature_shapes: List["tensorflow.TensorShape"] = None,
              label_column: str = None,
              label_type: "tensorflow.DType" = None,
              label_shape: "tensorflow.TensorShape" = None,
              shuffle: bool = True):
        from raydp.tf import TFDataset
        tf_dataset = TFDataset(ds=self,
                               feature_columns=feature_columns,
                               feature_types=feature_types,
                               feature_shapes=feature_shapes,
                               label_column=label_column,
                               label_type=label_type,
                               label_shape=label_shape,
                               shuffle=shuffle)
        return tf_dataset.setup_dataset(shard_id)
