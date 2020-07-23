from typing import Dict, Any

import jnius_config
import ray
from pyspark import find_spark_home
from pyspark.sql.session import SparkSession

import os

from raydp.services import Cluster
from raydp.spark.resource_manager.ray.app_master import AppMaster


class RayCluster(Cluster):
    def __init__(self):
        super().__init__(None)
        self._app_master = None
        self._set_up_master()
        self._spark_session: SparkSession = None

    def _set_up_master(self, resources: Dict[str, float], kwargs: Dict[Any, Any]):
        cp_list = self._prepare_jvm_classpath()
        self._prepare_jvm_options(cp_list)
        cp_str = os.pathsep.join(cp_list)
        self._app_master = AppMaster.createAppMaster(cp_str)

    def _prepare_jvm_options(self, jvm_cp_list):
        # TODO: set app master resource

        jnius_config.add_classpath(".")
        for cp in jvm_cp_list:
            jnius_config.add_classpath(cp)

        options = {}

        node = ray.worker.global_worker.node
        options["ray.node-ip"] = node.node_ip_address
        options["ray.redis.address"] = node.redis_address
        options["ray.redis.password"] = node.redis_password

        for key, value in options.items():
            jnius_config.add_options(f"-D{key}={value}")

    def _prepare_jvm_classpath(self):
        cp_list = []
        current_path = os.path.abspath(__file__)
        # find RayDP core path
        raydp_cp = os.path.join(current_path, "../../../../jars/*")
        cp_list.append(raydp_cp)
        # find pyspark jars path
        spark_home = find_spark_home._find_spark_home()
        spark_jars = os.path.join(spark_home, "jars/*")
        cp_list.append(spark_jars)
        return cp_list

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
