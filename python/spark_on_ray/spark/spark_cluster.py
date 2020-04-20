from spark_on_ray.cluster import Cluster
from typing import Any, Dict
from spark_on_ray.ray_cluster_resources import ClusterResources
from spark_on_ray.spark.spark_master_service import SparkMasterService
from spark_on_ray.spark.spark_worker_service import SparkWorkerService

import pyspark
from pyspark.sql import SparkSession
import ray

default_config = {
    "spark.sql.execution.arrow.enabled": "true"
}

_global_broadcasted = None


class SparkCluster(Cluster):
    def __init__(self,
                 ray_redis_address: str,
                 ray_redis_password: str,
                 master_resources: Dict[str, float],
                 spark_home: str,
                 master_port: int = 7077,
                 master_webui_port: Any = None,
                 master_properties: Dict[str, str] = None):
        self._ray_redis_address = ray_redis_address
        self._ray_redis_password = ray_redis_password

        self._spark_home = spark_home
        self._master_port = master_port
        self._master_webui_port = master_webui_port
        self._master_properties = master_properties

        self._master = None
        self._workers = []

        self._stopped = False

        super().__init__(master_resources)

        self._set_up_master(master_resources, None)

    def _set_up_master(self, resources: Dict[str, float], kwargs: Dict[str, str]):
        self._resource_check(resources)
        # set up master
        master_remote = ray.remote(**resources)(SparkMasterService)
        self._master = master_remote.remote(
            self._spark_home, self._master_port, self._master_webui_port, self._master_properties)
        # start up master
        ray.get(self._master.start_up.remote())
        # get master url after master start up
        self._master_url = ray.get(self._master.get_master_url.remote())

    def _set_up_worker(self, resources: Dict[str, float], kwargs: Dict[str, str]):
        # set up workers
        total_alive_nodes = ClusterResources.total_alive_nodes()
        if total_alive_nodes <= self._num_nodes:
            raise Exception("Don't have enough nodes,"
                            f"available: {total_alive_nodes}, request: {self._num_nodes}")
        self._resource_check(resources)

        port = kwargs.get("port", None)
        webui_port = kwargs.get("webui_port", None)
        properties = kwargs.get("properties", None)
        worker_remote = ray.remote(**resources)(SparkWorkerService)
        worker = worker_remote.remote(master_url=self._master_url,
                                      cores=resources["num_cpus"],
                                      memory=resources["memory"],
                                      spark_home=self._spark_home,
                                      port=port,
                                      webui_port=webui_port,
                                      properties=properties)
        # startup worker
        worker.start_up.remote()
        self._workers.append(worker)

    def get_cluster_url(self) -> str:
        return self._master_url

    def get_spark_session(self,
                          ray_redis_address: str,
                          ray_redis_password: str,
                          app_name: str,
                          num_executors: int,
                          executor_cores: int,
                          executor_memory: str,
                          **kwargs) -> pyspark.sql.SparkSession:
        conf = default_config
        conf.update(kwargs)

        builder = SparkSession.builder.appName(app_name).master(self.get_cluster_url())

        builder.config("spark.executor.instances", num_executors)
        builder.config("spark.executor.cores", executor_cores)
        builder.config("spark.executor.memory", executor_memory)

        for k, v in conf.items():
            builder.config(k, v)

        spark = builder.getOrCreate()

        # broadcast redis address and password
        global _global_broadcasted
        value = {"address": self._ray_redis_address, "password": self._ray_redis_password}
        _global_broadcasted = spark.sparkContext.broadcast(value)

        return spark

    def stop(self):
        if not self._stopped:
            # kill master
            if self._master:
                ray.get(self._master.stop.remote())

            if self._workers:
                ray.get([worker.stop.remote() for worker in self._workers])

            del self._master
            for worker in self._workers:
                del worker

            self._master_url = None
            self._stopped = True
            global _global_broadcasted_redis_address
            _global_broadcasted_redis_address = None
