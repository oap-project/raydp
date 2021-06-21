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

import json
import logging
import os
import shutil
import signal
import struct
import tempfile
import time
from subprocess import Popen, PIPE
from copy import copy
import glob

import pyspark
import ray
import ray.services

from raydp.services import ClusterMaster

logger = logging.getLogger(__name__)

RAYDP_JARS = os.path.abspath(os.path.join(os.path.abspath(__file__), "../../jars/*"))

class RayClusterMaster(ClusterMaster):
    def __init__(self, configs):
        self._host = None
        self._started_up = False
        self._configs = configs

    def start_up(self, popen_kwargs=None):
        if self._started_up:
            logger.warning("The RayClusterMaster has started already. Do not call it twice")
            return
        appMasterCls = ray.java_actor_class("org.apache.spark.deploy.raydp.RayAppMaster")
        self._host = ray.util.get_node_ip_address()
        self._instance = appMasterCls.remote()
        self._master_url = ray.get(self._instance.getMasterUrl.remote())
        extra_classpath = os.pathsep.join(self._prepare_jvm_classpath())
        # set classpath for executors
        ray.get(self._instance.setActorClasspath.remote(extra_classpath))
        self._started_up = True

    def _prepare_jvm_classpath(self):
        cp_list = []
        # find RayDP core path
        cp_list.append(RAYDP_JARS)
        # find ray jar path
        ray_jars = os.path.abspath(os.path.join(os.path.dirname(ray.__file__), "jars/*"))
        cp_list.append(ray_jars)
        # find pyspark jars path
        spark_home = os.path.dirname(pyspark.__file__)
        spark_jars_dir = os.path.join(spark_home, "jars/*")
        spark_jars = [jar for jar in glob.glob(spark_jars_dir) if "slf4j-log4j" not in jar]
        cp_list.extend(spark_jars)
        return cp_list

    def get_host(self) -> str:
        assert self._started_up
        return self._host

    def get_master_url(self):
        assert self._started_up
        return self._master_url

    def stop(self):
        if not self._started_up:
            return
        self._instance.stop.remote()
        self._started_up = False
