import os
from typing import Dict, Any

import jnius_config
import json
import ray
import ray.services
from pyspark import find_spark_home
from pyspark.sql.session import SparkSession

from raydp.services import Cluster


class RayCluster(Cluster):
    def __init__(self):
        super().__init__(None)
        self._app_master_bridge = None
        self._set_up_master(None, None)
        self._spark_session: SparkSession = None

    def _set_up_master(self, resources: Dict[str, float], kwargs: Dict[Any, Any]):
        cp_list = self._prepare_jvm_classpath()
        jvm_properties = self._prepare_jvm_options(cp_list)
        cp_str = os.pathsep.join(cp_list)
        from raydp.spark.resource_manager.ray.app_master_py_bridge import AppMasterPyBridge
        self._app_master_bridge = AppMasterPyBridge(jvm_properties)
        self._app_master_bridge.create_app_master(cp_str)

    def _prepare_jvm_options(self, jvm_cp_list):
        # TODO: set app master resource

        jnius_config.add_classpath(".", *jvm_cp_list)

        options = {}

        node = ray.worker.global_worker.node

        options["ray.node-ip"] = node.node_ip_address
        options["ray.redis.address"] = node.redis_address
        options["ray.redis.password"] = node.redis_password
        options["ray.redis.head-password"] = node.redis_password
        options["ray.logging.dir"] = node.get_session_dir_path()
        options["ray.session-dir"] = node.get_session_dir_path()
        options["ray.raylet.node-manager-port"] = node.node_manager_port
        options["ray.raylet.socket-name"] = node.raylet_socket_name
        options["ray.object-store.socket-name"] = node.plasma_store_socket_name

        # jnius_config.set_option has some bug, we set this options in java side
        jvm_properties = json.dumps(options)
        print(jvm_properties)
        return jvm_properties

    def _prepare_jvm_classpath(self):
        cp_list = []
        # find ray jar path
        cp_list.extend(ray.services.DEFAULT_JAVA_WORKER_CLASSPATH)
        # find RayDP core path
        current_path = os.path.abspath(__file__)
        raydp_cp = os.path.abspath(os.path.join(current_path, "../../../../jars/*"))
        cp_list.append(raydp_cp)
        # find pyspark jars path
        spark_home = find_spark_home._find_spark_home()
        spark_jars = os.path.join(spark_home, "jars/*")
        cp_list.append(spark_jars)
        return cp_list

    def _set_up_worker(self, resources: Dict[str, float], kwargs: Dict[str, str]):
        raise Exception("Unsupported operation")

    def get_cluster_url(self) -> str:
        self._app_master_bridge.get_master_url()

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

        if self._app_master_bridge is not None:
            self._app_master_bridge.stop()
            self._app_master_bridge = None
