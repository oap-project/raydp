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

from typing import Callable, List, Iterator, Optional

from raydp.parallel.interfaces import T, _Shard

import ray


class SourceShard(_Shard[T]):
    def __init__(self, name: str, is_repeatable: bool):
        self._name = name
        self._is_repeatable = is_repeatable

    def name(self) -> str:
        return self._name

    def repeatable(self) -> bool:
        """
        Whether this source shard could generate source data repeatable.
        """
        return self._is_repeatable

    def gen_data(self, **kwargs) -> Iterator[T]:
        raise NotImplementedError

    def to_torch(self, **kwargs):
        raise TypeError("unsupported operation")

    def to_tf(self, **kwargs):
        raise TypeError("unsupported operation")

    def __str__(self):
        return repr(self)

    def __repr__(self):
        return self.name()


class GeneratorSource(SourceShard[T]):
    def __init__(self,
                 name: str,
                 generator: Callable[[], Iterator[T]],
                 is_repeatable: bool):
        super(GeneratorSource, self).__init__(name, is_repeatable)
        self._generator = generator

    def gen_data(self, **kwargs) -> Iterator[T]:
        return self._generator(**kwargs)


class CacheDataSource(SourceShard[T]):
    def __init__(self,
                 object_ids: List[ray.ObjectRef],
                 read_fn: Callable[[ray.ObjectRef], T],
                 name: str = "CacheData",
                 is_repeatable: bool = True):
        super(CacheDataSource, self).__init__(name, is_repeatable)
        self._object_ids = object_ids
        self._read_fn = read_fn

    def gen_data(self, **kwargs):
        for object_id in self._object_ids:
            yield self._read_fn(object_id)
