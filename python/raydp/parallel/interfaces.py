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

from typing import Generic, TypeVar

# The type of an iterator element.
T = TypeVar("T")
U = TypeVar("U")


class _Shard(Generic[T]):

    def to_torch(self, **kwargs):
        """
        Create a torch Dataset from the current shard
        :return: a torch.utils.data.Dataset
        """
        raise NotImplementedError

    def to_tf(self, **kwargs):
        """
        Create a tensorflow Dataset from the current shard
        :return: a tensorflow.data.Dataset
        """
        raise NotImplementedError


class _Dataset(Generic[T]):

    def get_shard(self, shard_id: int) -> _Shard[T]:
        """
        Get one piece of Shards
        """
        raise NotImplementedError

    def num_shards(self) -> int:
        raise NotImplementedError

    def to_torch(self, **kwargs):
        """
        Create a torch Dataset from the current dataset.
        :return: a torch.utils.data.Dataset
        """
        pass

    def to_tf(self, **kwargs):
        """
        Create a tensorflow Dataset from the current dataset
        :return: a tensorflow.data.Dataset
        """
        pass
