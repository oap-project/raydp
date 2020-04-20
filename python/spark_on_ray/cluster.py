from typing import Any, Dict, Type

from abc import ABC, abstractmethod
from ray_cluster_resources import ClusterResources


class Cluster(ABC):
    """
    This is the base class for all specified cluster, such as SparkCluster, FlinkCluster.

    :param master_resources_requirement: The resources requirement for the master service.
    """
    def __init__(self, master_resources_requirement):
        # the master node is live as same as ray driver node. And we can specify the resources
        # limitation for master node. So we don't count it.
        self._num_nodes = 0

    def _resource_check(self, resources: Dict[str, float]):
        # check whether this is any node could satisfy the master service requirement
        if not ClusterResources.satisfy(resources):
            raise Exception("There is not any node can satisfy the service resources "
                            f"requirement, request: {resources}")

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
        self._set_up_worker(resources_requirement, kwargs)

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
