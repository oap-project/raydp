from typing import Any, Dict

import ray
from ray.ray_constants import MEMORY_RESOURCE_UNIT_BYTES
import time


class ClusterResources:
    # TODO: make this configurable
    refresh_interval = 0.1
    latest_refresh_time = time.time()
    node_to_resources = {}
    item_keys_mapping = {"num_cpus": "CPU"}

    @classmethod
    def total_alive_nodes(cls):
        cls._refresh()
        return len(cls.node_to_resources)

    @classmethod
    def satisfy(cls, request: Dict[str, float]) -> Any:
        cls._refresh()
        for host_name, resources in cls.node_to_resources.items():
            if cls._compare_two_dict(resources, request):
                return host_name

        return None

    @classmethod
    def _refresh(cls):
        if (time.time() - cls.latest_refresh_time) < cls.refresh_interval:
            return

        for node in ray.nodes():
            if node["Alive"]:
                host_name = node["NodeManagerHostname"]
                resources = node["Resources"]
                cls.node_to_resources[host_name] = resources
        cls.latest_refresh_time = time.time()

    @classmethod
    def _compare_two_dict(cls, available: Dict[str, float], request: Dict[str, float]) -> bool:
        for k, v in request.items():
            k = cls.item_keys_mapping.get(k, k)
            if k not in available:
                return False

            if k == "memory":
                v = int(v / MEMORY_RESOURCE_UNIT_BYTES)

            if available[k] < v:
                return False

        return True
