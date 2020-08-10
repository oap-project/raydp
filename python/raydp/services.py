from abc import ABC, abstractmethod
from typing import Any, Dict


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
        pass

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
        pass

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
    def start_up(self) -> str:
        """
        :return: error message, return None if succeeded
        """
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
        pass

    @abstractmethod
    def get_host(self) -> str:
        pass

    @abstractmethod
    def stop(self):
        pass
