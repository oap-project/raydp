from typing import Any, Dict

import ray
import time


class ClusterResources:
    # TODO: make this configurable
    refresh_interval = 0.1
    latest_refresh_time = time.time()
    node_to_resources = {}

    @staticmethod
    def total_alive_nodes(cls):
        cls._refresh()
        return len(cls.node_to_resources)

    @staticmethod
    def satisfy(cls, request: Dict[str, float]) -> Any:
        cls._refresh()
        for host_name, resources in cls.node_to_resources.items():
            if cls._compare_two_dict(resources, request):
                return host_name

        return None

    @staticmethod
    def _refresh(cls):
        if (time.time() - cls.latest_refresh_time) < cls.refresh_interval:
            return

        for node in ray.nodes():
            if node["Alive"]:
                host_name = node["NodeManagerHostname"]
                resources = node["Resources"]
                cls.node_to_resources[host_name] = resources
        cls.latest_refresh_time = time.time()

    @staticmethod
    def _compare_two_dict(available: Dict[str, float], request: Dict[str, float]) -> bool:
        for k, v in request.items():
            if k not in available:
                return False

            if available[k] < v:
                return False

        return True
