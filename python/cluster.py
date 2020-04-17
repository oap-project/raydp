from typing import Any, Dict, Type

from abc import ABC, abstractmethod
from ray_cluster_resources import ClusterResources


class Cluster(ABC):
    """
    This is the base class for all specified cluster, such as SparkCluster, FlinkCluster.

    :param num_nodes: the total number of nodes request, this should the size of worker + 1
    :param master_class: the master actor class that will startup subcluster master service
    :param master_resources: the resources requirement for the master service
    :param worker_class: the worker actor class that will startup subcluster worker service
    :param worker_resources_mapping: the resources mapping between worker index to resources
                                     requirement. The size should be equal as workers.
    """
    def __init__(self,
                 num_nodes: int,
                 master_class: Type,
                 master_resources: Dict[str, float],
                 worker_class: Type,
                 worker_resources_mapping: Dict[int, Dict[str, float]]):
        self._num_nodes = num_nodes
        self._master_class = master_class
        self._master_resources = master_resources
        self._worker_class = worker_class
        self._worker_resources_mapping = worker_resources_mapping

        assert num_nodes == (len(self._master_resources) + 1)
        self._resource_check()

    def _resource_check(self):
        total_alive_nodes = ClusterResources.total_alive_nodes()
        # check nodes request can be satisfied
        assert total_alive_nodes >= self._num_nodes, f"Don't have enough nodes, "\
            f"available: {total_alive_nodes}, request: {self._num_nodes}"

        # check whether this is any node could satisfy the master service requirement
        if not ClusterResources.satisfy(self._master_resources):
            raise Exception("There is not any node can satisfy the master service resources "
                            f"requirement, request: {self._master_resources}")

        for index, requirement in self._worker_resources_mapping.items():
            if not ClusterResources.satisfy(requirement):
                raise Exception(f"There is not any node can satisfy the {index}th worker service "
                                f"resources requirement, request: {requirement}")

    @abstractmethod
    def _set_up(self):
        """
        Subcluster should implement this to set up cluster.
        """
        pass

    @abstractmethod
    def get_cluster_url(self) -> str:
        """
        Return the cluster url, eg: spark://master-node:7077
        """

    @abstractmethod
    def kill(self):
        """
        Kill cluster
        """






