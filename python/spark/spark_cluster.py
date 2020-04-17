from cluster import Cluster
from typing import Dict, Type
from spark.spark_master_service import SparkMasterService
from spark.spark_worker_service import SparkWorkerService

from pyspark.sql import SparkSession
import ray


class SparkCluster(Cluster):
    def __init__(self,
                 num_nodes: int,
                 master_resources: Dict[str, float],
                 worker_resources_mapping: Dict[int, Dict[str, float]],
                 spark_home: str,
                 master_port: int = 7077,
                 master_webui_port: int = None,
                 master_properties: Dict[str, str] = None,
                 worker_properties: Dict[int, Dict[str, str]] = None):
        super(SparkCluster).__init__(num_nodes, SparkMasterService, master_resources,
                                     SparkWorkerService, worker_resources_mapping)
        self._spark_home = spark_home
        self._master_port = master_port
        self._master_webui_port = master_webui_port
        self._master_properties = master_properties
        self._worker_properties = worker_properties

        self._master = None
        self._workers = None

        self._killed = False

        self._set_up()

    def _set_up(self):
        # set up master
        master_remote = ray.remote(**self._master_resources)(self._master_class)
        self._master = master_remote.remote(
            self._spark_home, self._master_port, self._master_webui_port, self._master_properties)
        # start up master
        ray.get(self._master.start_up())
        # get master url after master start up
        self._master_url = ray.get(self._master.get_master_url())

        # set up workers
        self._workers = []
        for i in range(self._num_nodes - 1):
            resources = self._worker_resources_mapping[i]
            properties = self._worker_properties.get(i, None)
            worker_remote = ray.remote(resources)(self._worker_class)
            worker = worker_remote.remote(master_url=self._master_url,
                                          cores=resources["num_cpus"],
                                          memory=resources["memory"],
                                          spark_home=self._spark_home,
                                          port=None,
                                          webui_port=None,
                                          properties=properties)
            # startup worker
            worker.start_up.remote()
            self._workers.append(worker)

    def get_cluster_url(self) -> str:
        return self._master_url

    def get_spark_session(self, app_name, **kwargs):
        builder = SparkSession.Builder
        builder.appName(app_name)\
               .master(self.get_cluster_url())

        for k, v in kwargs.items():
            builder.config(k, v)

        return builder.getOrCreate()

    def kill(self):
        if not self._killed:
            # kill master
            if self._master:
                ray.get(self._master.kill.remote())

            if self._workers:
                ray.get([worker.kill.remote() for worker in self._workers])

            del self._master
            for worker in self._workers:
                del worker





