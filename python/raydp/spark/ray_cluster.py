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
from typing import Any, Dict

from pyspark.sql.session import SparkSession

from .ray_cluster_master import RayClusterMaster, RAYDP_CP
from raydp.services import Cluster


class SparkCluster(Cluster):
    def __init__(self):
        super().__init__(None)
        self._app_master_bridge = None
        self._set_up_master(None, None)
        self._spark_session: SparkSession = None

    def _set_up_master(self, resources: Dict[str, float], kwargs: Dict[Any, Any]):
        # TODO: specify the app master resource
        self._app_master_bridge = RayClusterMaster()
        self._app_master_bridge.start_up()

    def _set_up_worker(self, resources: Dict[str, float], kwargs: Dict[str, str]):
        raise Exception("Unsupported operation")

    def get_cluster_url(self) -> str:
        return self._app_master_bridge.get_master_url()

    def get_spark_session(self,
                          app_name: str,
                          num_executors: int,
                          executor_cores: int,
                          executor_memory: int,
                          extra_conf: Dict[str, str] = None) -> SparkSession:
        if self._spark_session is not None:
            return self._spark_session

        if extra_conf is None:
            extra_conf = {}
        extra_conf["spark.executor.instances"] = str(num_executors)
        extra_conf["spark.executor.cores"] = str(executor_cores)
        extra_conf["spark.executor.memory"] = str(executor_memory)
        extra_conf["spark.jars"] = ",".join(glob.glob(RAYDP_CP))
        spark_builder = SparkSession.builder
        for k, v in extra_conf.items():
            spark_builder.config(k, v)
        self._spark_session =\
            spark_builder.appName(app_name).master(self.get_cluster_url()).getOrCreate()
        return self._spark_session

    def stop(self):
        if self._spark_session is not None:
            self._spark_session.stop()
            self._spark_session = None

        if self._app_master_bridge is not None:
            self._app_master_bridge.stop()
            self._app_master_bridge = None
