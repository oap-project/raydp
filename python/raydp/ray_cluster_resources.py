from typing import Dict, List

import ray
import time
from ray.ray_constants import MEMORY_RESOURCE_UNIT_BYTES


class ClusterResources:
    # TODO: make this configurable
    refresh_interval = 0.1
    latest_refresh_time = time.time() - refresh_interval
    node_to_resources = {}
    item_keys_mapping = {"num_cpus": "CPU"}
    label_name = "__ray_spark_node_label"

    @classmethod
    def total_alive_nodes(cls):
        cls._refresh()
        return len(cls.node_to_resources)

    @classmethod
    def satisfy(cls, request: Dict[str, float]) -> List[str]:
        cls._refresh()
        satisfied = []
        for host_name, resources in cls.node_to_resources.items():
            if cls._compare_two_dict(resources, request):
                satisfied.append(resources[cls.label_name])

        return satisfied

    @classmethod
    def _refresh(cls):
        if (time.time() - cls.latest_refresh_time) < cls.refresh_interval:
            return

        for node in ray.nodes():
            if node["Alive"]:
                host_name = node["NodeManagerHostname"]
                resources = node["Resources"]
                for key in resources:
                    if key.startswith("node:"):
                        resources[cls.label_name] = key
                        break
                assert cls.label_name in resources,\
                    f"{resources} should contain a resource likes: 'node:10.0.0.131': 1.0"
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
