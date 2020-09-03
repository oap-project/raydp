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

from typing import Dict, List, NoReturn, Optional

import ray
import ray.cloudpickle as rpickle
import ray.worker
import time


@ray.remote(num_cpus=0)
def save(value: List[ray.ObjectID]) -> List[ray.ObjectID]:
    value = value[0]
    tmp = ray.get(value)
    result = ray.put(tmp)
    return [result]


@ray.remote(num_cpus=0)
class BlockHolder:
    """
    A block holder alive on each node.
    """
    def __init__(self, node_label: str, concurrent_save=10, save_interval=5):
        # hold the ObjectID to increase the ray inner reference counter
        self._data: Dict[int, ray.ObjectID] = {}
        self._data_reference_counter: Dict[int, int] = {}
        self._fetch_index: int = 0

        self._save_fn = save.options(resources={node_label: 0.01})
        self._concurrent_save = concurrent_save
        self._save_interval = save_interval
        self._save_pool = []
        self._save_pool_fetch_index = []
        self._last_save_time = time.time()

    def _update_remote_save(self):
        objs = ray.get(self._save_pool)
        for index, obj in zip(self._save_pool_fetch_index, objs):
            self._data[index] = obj[0]
        self._save_pool = []
        self._save_pool_fetch_index = []
        self._last_save_time = time.time()

    def _pin_object(self, object_id: ray.ObjectID, fetch_index: int):
        """
        TODO: add support object owner transfer in ray
        Pin the object in block holder, this should be fixed when we support transfer object
        owner in ray.
        :param object_id: the original object id
        :param fetch_index: the fetch index that can fetch the given data
        """
        cur_time = time.time()
        if ((len(self._save_pool) > 0 and len(self._save_pool) == self._concurrent_save)
                or (self._last_save_time - cur_time) > self._save_interval):
            self._update_remote_save()

        remote_id = self._save_fn.remote([object_id])
        self._save_pool.append(remote_id)
        self._save_pool_fetch_index.append(fetch_index)

    def register_object_id(self, object_ids: List[ray.ObjectID]) -> List[int]:
        """
        Register a list of object id to hold.
        :param object_ids: list of ray object id
        :return: fetch indexes that can fetch the given value with get_object
        """
        results = []
        for object_id in object_ids:
            fetch_index = self._fetch_index
            self._fetch_index += 1
            self._pin_object(object_id, fetch_index)
            self._data_reference_counter[fetch_index] = 0
            results.append(fetch_index)

        if len(results) == 1:
            results = results[0]

        return results

    def get_object(self, fetch_indexes: List[int]) -> List[Optional[ray.ObjectID]]:
        """
        Get registered ObjectId. This will increase the ObjectId reference counter.
        :param fetch_indexes: the fetch index which used to look the mapping ObjectId
        :return: ObjectId or None if it is not found
        """
        results = []
        for index in fetch_indexes:
            if index in self._save_pool_fetch_index:
                self._update_remote_save()

            if index not in self._data:
                results.append(None)
            else:
                # increase the reference counter
                self._data_reference_counter[index] =\
                    self._data_reference_counter[index] + 1
                results.append(self._data[index])

        return results

    def remove_object_id(self, fetch_index: int, destroy: bool = False) -> NoReturn:
        """
        Remove the reference for the ObjectId
        :param fetch_index: the fetch index which used to look the mapping ObjectId
        :param destroy: whether destroy the ObjectId when the reference counter is zero
        """
        if fetch_index in self._save_pool_fetch_index:
            self._update_remote_save()

        if fetch_index in self._data:
            assert self._data_reference_counter[fetch_index] > 0
            self._data_reference_counter[fetch_index] =\
                self._data_reference_counter[fetch_index] - 1

            if destroy and self._data_reference_counter[fetch_index] == 0:
                self.destroy_object_id(fetch_index)

    def remove_object_ids(self, fetch_indexes: List[int], destroy: bool = False) -> NoReturn:
        for fetch_index in fetch_indexes:
            self.remove_object_id(fetch_index, destroy)

    def destroy_object_id(self, fetch_index) -> NoReturn:
        """
        Destroy the ObjectId directly.
        :param fetch_index: the fetch index which used to look the mapping ObjectId
        """
        if fetch_index in self._save_pool_fetch_index:
            self._update_remote_save()

        if fetch_index in self._data:
            del self._data[fetch_index]
            del self._data_in_bytes[fetch_index]
            del self._data_reference_counter[fetch_index]

    def destroy_object_ids(self, fetch_indexes: List[int]) -> NoReturn:
        for fetch_index in fetch_indexes:
            self.destroy_object_id(fetch_index)

    def stop(self) -> None:
        """
        Clean all data.
        """
        self._update_remote_save()
        self.destroy_object_ids(list(self._data.keys()))


class BlockHolderActorHandlerWrapper:
    """
    A BlockHolder actor handler wrapper to support deserialize the the handler after
    ray initialized.
    """
    def __init__(self, block_holder: BlockHolder):
        """
        :param block_holder: the BlockHolder actor handler
        """
        self._block_holder = block_holder

    def __getattr__(self, item):
        if not self._block_holder:
            self._lazy_deserialize()

        return getattr(self._block_holder, item)

    @classmethod
    def _custom_deserialize(cls, serialized_data):
        instance = cls(None)
        instance._serialized_data = serialized_data
        return instance

    def _lazy_deserialize(self):
        """
        This should be called after ray has been initialized.
        """
        assert ray.is_initialized()
        self._block_holder = rpickle.loads(self._serialized_data)

    def __reduce__(self):
        serialized_data = rpickle.dumps(self._block_holder)
        return BlockHolderActorHandlerWrapper._custom_deserialize, (serialized_data,)
