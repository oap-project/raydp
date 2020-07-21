from typing import Dict, Any
from raydp.services import Cluster
from raydp.spark.resource_manager.ray.app_master import AppMaster

from pyspark.sql.session import SparkSession


class RayCluster(Cluster):
    def __init__(self):
        super().__init__(None)
        self._app_master = AppMaster()
        self._set_up_master()
        self._spark_session: SparkSession = None

    def _set_up_master(self, resources: Dict[str, float], kwargs: Dict[Any, Any]):
        self._app_master.startup_java_app_master()

    def _set_up_worker(self, resources: Dict[str, float], kwargs: Dict[str, str]):
        raise Exception("Unsupported operation")

    def get_cluster_url(self) -> str:
        self._app_master.get_master_url()

    def get_spark_session(self,
                          app_name: str,
                          num_executors: int,
                          executor_cores: int,
                          executor_memory: int,
                          extra_conf: Dict[str, str]) -> SparkSession:
        if self._spark_session is not None:
            return self._spark_session
        extra_conf["spark.executor.instances"] = str(num_executors)
        extra_conf["spark.executor.cores"] = str(executor_cores)
        extra_conf["spark.executor.memory"] = str(executor_memory)
        spark_builder = SparkSession.builder
        for k, v in extra_conf.items():
            spark_builder.config(k, v)
        self._spark_session = spark_builder.appName(app_name).master(self.get_cluster_url())
        return self._spark_session

    def stop(self):
        if self._spark_session is not None:
            self._spark_session.stop()
            self._spark_session = None

        if self._app_master is not None:
            self._app_master.stop()
            self._app_master = None

