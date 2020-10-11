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

from typing import Generic, List, Optional, TypeVar

# The type of an iterator element.
T = TypeVar("T")
U = TypeVar("U")


class _Shard(Generic[T]):

    def name(self) -> str:
        raise NotImplementedError

    def repeatable(self) -> bool:
        raise NotImplementedError

    def repeated(self) -> bool:
        raise NotImplementedError

    def repeat(self) -> "_Shard[T]":
        if self.repeated():
            # TODO: maybe fire a warning to user
            return self

        if not self.repeatable():
            raise Exception("can not repeat on an unrepeatable shard")

    def __iter__(self):
        raise NotImplementedError

    def __str__(self):
        return repr(self)

    def __repr__(self):
        return self.name()


class _Dataset(Generic[T]):

    def name(self):
        """
        Return the Dataset name
        """
        raise NotImplementedError

    def repeated(self) -> bool:
        """Whether this Dataset is repeated."""
        raise NotImplementedError

    def repeatable(self) -> bool:
        """Whether this Dataset is repeatable, repeated dataset is not repeatable"""
        raise NotImplementedError

    def repeat(self) -> "_Dataset[T]":
        """
        Repeat the Dataset, this will only apply to source dataset. If the source dataset is
        not repeatable, it will raise exception.
        :return a repeated Dataset
        """
        raise NotImplementedError

    def get_shard(self, shard_id: int) -> _Shard[T]:
        """
        Get one piece of Shards
        """
        raise NotImplementedError

    def num_shards(self) -> int:
        raise NotImplementedError

    def to_torch(self, shard_ids: Optional[List[int]], **kwargs):
        """
        Create a torch Dataset from the current dataset.
        :param shard_ids create a torch Dataset from the given shard ids data. If the shard_ids
                         is None, we will create the torch Dataset from the full shards.
        :return: a torch.utils.data.Dataset
        """
        pass

    def to_tf(self, shard_ids: Optional[List[int]], **kwargs):
        """
        Create a tensorflow Dataset from the current dataset
        :param shard_ids create a tensorflow Dataset from the given shard ids data. If the
                         shard_ids is None, we will create the torch Dataset from the full
                         shards.
        :return: a tensorflow.data.Dataset
        """
        pass

    def __str__(self):
        return repr(self)

    def __repr__(self):
        return self.name()
