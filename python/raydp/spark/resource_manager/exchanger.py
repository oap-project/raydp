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

from abc import ABC, abstractmethod
from typing import List, NoReturn

import pandas as pd
import ray


class SharedDataset(ABC):
    """
    A SharedDataset means the Spark DataFrame that have been stored in Ray ObjectStore.
    """

    @abstractmethod
    def total_size(self) -> int:
        pass

    @abstractmethod
    def partition_sizes(self) -> List[int]:
        pass

    @staticmethod
    def _fetch_objects_without_deserialization(object_ids, timeout=None) -> NoReturn:
        """
        This is just fetch object from remote object store to local and without deserialization.
        :param object_ids: Object ID of the object to get or a list of object IDs to
            get.
        :param timeout (Optional[float]): The maximum amount of time in seconds to
            wait before returning.
        """
        is_individual_id = isinstance(object_ids, ray.ObjectID)
        if is_individual_id:
            object_ids = [object_ids]

        if not isinstance(object_ids, list):
            raise ValueError("'object_ids' must either be an object ID "
                             "or a list of object IDs.")

        worker = ray.worker.global_worker
        worker.check_connected()
        timeout_ms = int(timeout * 1000) if timeout else -1
        worker.core_worker.get_objects(object_ids, worker.current_task_id, timeout_ms)

    @abstractmethod
    def resolve(self, timeout=None) -> bool:
        pass

    @abstractmethod
    def set_plasma_store_socket_name(self, path: str) -> NoReturn:
        pass

    @abstractmethod
    def subset(self, indexes: List[int]) -> 'SharedDataset':
        pass

    @abstractmethod
    def __getitem__(self, item) -> pd.DataFrame:
        pass
