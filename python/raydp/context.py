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

import atexit
from contextlib import ContextDecorator
from threading import RLock
from typing import Dict, Union, Optional

import ray
from pyspark.sql import SparkSession

from raydp.spark import SparkCluster
from raydp.utils import parse_memory_size


class _SparkContext(ContextDecorator):
    """A class used to create the Spark cluster and get the Spark session.

    :param app_name the Spark application name
    :param num_executors the number of executor requested
    :param executor_cores the CPU cores for each executor
    :param executor_memory the memory size (eg: 10KB, 10MB..) for each executor
    :param configs the extra Spark configs need to set
    """
    def __init__(self,
                 app_name: str,
                 num_executors: int,
                 executor_cores: int,
                 executor_memory: Union[str, int],
                 configs: Dict[str, str] = None):
        self._app_name = app_name
        self._num_executors = num_executors
        self._executor_cores = executor_cores

        if isinstance(executor_memory, str):
            # If this is human readable str(like: 10KB, 10MB..), parse it
            executor_memory = parse_memory_size(executor_memory)

        self._executor_memory = executor_memory
        self._configs = {} if configs is None else configs

        self._spark_cluster: Optional[SparkCluster] = None
        self._spark_session: Optional[SparkSession] = None

    def _get_or_create_spark_cluster(self) -> SparkCluster:
        if self._spark_cluster is not None:
            return self._spark_cluster
        self._spark_cluster = SparkCluster(self._configs)
        return self._spark_cluster

    def get_or_create_session(self):
        if self._spark_session is not None:
            return self._spark_session
        spark_cluster = self._get_or_create_spark_cluster()
        self._spark_session = spark_cluster.get_spark_session(
            self._app_name,
            self._num_executors,
            self._executor_cores,
            self._executor_memory,
            self._configs)
        return self._spark_session

    def stop(self):
        if self._spark_session is not None:
            self._spark_session.stop()
            self._spark_session = None
        if self._spark_cluster is not None:
            self._spark_cluster.stop()
            self._spark_cluster = None

    def __enter__(self):
        self.get_or_create_session()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()


_spark_context_lock = RLock()
_global_spark_context: _SparkContext = None


def init_spark(app_name: str,
               num_executors: int,
               executor_cores: int,
               executor_memory: Union[str, int],
               configs: Optional[Dict[str, str]] = None):
    """
    Init a Spark cluster with given requirements.
    :param app_name: The application name.
    :param num_executors: number of executor requests
    :param executor_cores: the number of CPU cores for each executor
    :param executor_memory: the memory size for each executor, both support bytes or human
                            readable string.
    :param configs: the extra Spark config need to set
    :return: return the SparkSession
    """

    if not ray.is_initialized():
        # ray has not initialized, init local
        ray.init()

    with _spark_context_lock:
        global _global_spark_context
        if _global_spark_context is None:
            try:
                _global_spark_context = _SparkContext(
                    app_name, num_executors, executor_cores, executor_memory, configs)
                return _global_spark_context.get_or_create_session()
            except:
                _global_spark_context = None
                raise
        else:
            raise Exception("The spark environment has inited.")


def stop_spark():
    with _spark_context_lock:
        global _global_spark_context
        if _global_spark_context is not None:
            _global_spark_context.stop()
            _global_spark_context = None


atexit.register(stop_spark)
