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
from typing import Any, Dict, NoReturn


class Cluster(ABC):
    """
    This is the base class for all specified cluster, such as SparkCluster, FlinkCluster.
    :param master_resources_requirement: The resources requirement for the master service.
    """
    def __init__(self, master_resources_requirement):
        # the master node is live as same as ray driver node. And we can specify the resources
        # limitation for master node. So we don't count it.
        self._num_nodes = 0

    @abstractmethod
    def _set_up_master(self,
                       resources: Dict[str, float],
                       kwargs: Dict[Any, Any]):
        """
        Subcluster should implement this to set up master node.
        """

    def add_worker(self,
                   resources_requirement: Dict[str, float],
                   **kwargs: Dict[Any, Any]):
        """
        Add one worker to the cluster.
        :param resources_requirement: The resource requirements for the worker service.
        """
        try:
            self._set_up_worker(resources_requirement, kwargs)
        except:
            self.stop()
            raise

    @abstractmethod
    def _set_up_worker(self,
                       resources: Dict[str, float],
                       kwargs: Dict[str, str]):
        """
        Subcluster should implement this to set up worker node.
        """

    @abstractmethod
    def get_cluster_url(self) -> str:
        """
        Return the cluster url, eg: spark://master-node:7077
        """

    @abstractmethod
    def stop(self):
        """
        Stop cluster
        """


class ClusterMaster(ABC):

    @abstractmethod
    def start_up(self) -> NoReturn:
        pass

    @abstractmethod
    def get_master_url(self) -> str:
        pass

    @abstractmethod
    def get_host(self) -> str:
        pass

    @abstractmethod
    def stop(self):
        pass


class ClusterWorker(ABC):

    @abstractmethod
    def start_up(self) -> str:
        """
        :return: error message, return None if succeeded
        """

    @abstractmethod
    def get_host(self) -> str:
        pass

    @abstractmethod
    def stop(self):
        pass
