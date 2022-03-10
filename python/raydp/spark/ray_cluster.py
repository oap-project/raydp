#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import glob
import os
from typing import Any, Dict, Optional

import ray
import ray._private.services
from pyspark.sql.session import SparkSession

from raydp.services import Cluster
from .ray_cluster_master import RAYDP_SPARK_MASTER_NAME, RayDPSparkMaster


class SparkCluster(Cluster):
    def __init__(self, configs):
        super().__init__(None)
        self._spark_master_handle = None
        self._configs = configs
        self._set_up_master(None, None)
        self._spark_session: SparkSession = None

    def _set_up_master(self, resources: Dict[str, float], kwargs: Dict[Any, Any]):
        # TODO: specify the app master resource
        self._spark_master_handle = RayDPSparkMaster.options(name=RAYDP_SPARK_MASTER_NAME) \
                                                   .remote(self._configs)
        self._spark_master_handle.start_up.remote()

    def _set_up_worker(self, resources: Dict[str, float], kwargs: Dict[str, str]):
        raise Exception("Unsupported operation")

    def get_cluster_url(self) -> str:
        return ray.get(self._spark_master_handle.get_master_url.remote())

    def get_spark_session(self,
                          app_name: str,
                          num_executors: int,
                          executor_cores: int,
                          executor_memory: int,
                          enable_hive: bool,
                          extra_conf: Dict[str, str] = None) -> SparkSession:
        if self._spark_session is not None:
            return self._spark_session

        if extra_conf is None:
            extra_conf = {}
        extra_conf["spark.executor.instances"] = str(num_executors)
        extra_conf["spark.executor.cores"] = str(executor_cores)
        extra_conf["spark.executor.memory"] = str(executor_memory)
        driver_node_ip = ray._private.services.get_node_ip_address()
        extra_conf["spark.driver.host"] = str(driver_node_ip)
        extra_conf["spark.driver.bindAddress"] = str(driver_node_ip)
        RAYDP_CP = os.path.abspath(os.path.join(os.path.abspath(__file__), "../../jars/*"))
        RAY_CP = os.path.abspath(os.path.join(os.path.dirname(ray.__file__), "jars/*"))
        try:
            extra_jars = [extra_conf["spark.jars"]]
        except KeyError:
            extra_jars = []
        extra_conf["spark.jars"] = ",".join(glob.glob(RAYDP_CP) + extra_jars)
        driver_cp_key = "spark.driver.extraClassPath"
        driver_cp = ":".join(glob.glob(RAYDP_CP) + glob.glob(RAY_CP))
        if driver_cp_key in extra_conf:
            extra_conf[driver_cp_key] = driver_cp + ":" + extra_conf[driver_cp_key]
        else:
            extra_conf[driver_cp_key] = driver_cp
        spark_builder = SparkSession.builder
        for k, v in extra_conf.items():
            spark_builder.config(k, v)
        if enable_hive:
            spark_builder.enableHiveSupport()
        self._spark_session = \
            spark_builder.appName(app_name).master(self.get_cluster_url()).getOrCreate()
        return self._spark_session

    def stop(self):
        if self._spark_session is not None:
            self._spark_session.stop()
            self._spark_session = None

        if self._spark_master_handle is not None:
            self._spark_master_handle.stop.remote()
            self._spark_master_handle = None
