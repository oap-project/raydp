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
from typing import Dict, List, Union, Optional

import ray
from pyspark.sql import SparkSession

from ray.util.placement_group import PlacementGroup

from raydp.spark import SparkCluster
from raydp.spark.dataset import RayDPConversionHelper, RAYDP_OBJ_HOLDER_NAME
from raydp.utils import parse_memory_size


class _SparkContext(ContextDecorator):
    """A class used to create the Spark cluster and get the Spark session.

    :param app_name the Spark application name
    :param num_executors the number of executor requested
    :param executor_cores the CPU cores for each executor
    :param executor_memory the memory size (eg: 10KB, 10MB..) for each executor
    :param placement_group_strategy: RayDP will create a placement group according to the
                                     strategy and the configured resources for executors.
                                     If this parameter is specified, the next two
                                     parameters placement_group and
                                     placement_group_bundle_indexes will be ignored.
    :param placement_group: placement_group to schedule executors on
    :param placement_group_bundle_indexes: which bundles to use. If it's not specified,
                                           all bundles will be used.
    :param configs the extra Spark configs need to set
    """

    _PLACEMENT_GROUP_CONF = "spark.ray.placement_group"
    _BUNDLE_INDEXES_CONF = "spark.ray.bundle_indexes"

    def __init__(self,
                 app_name: str,
                 num_executors: int,
                 executor_cores: int,
                 executor_memory: Union[str, int],
                 placement_group_strategy: Optional[str],
                 placement_group: Optional[PlacementGroup],
                 placement_group_bundle_indexes: Optional[List[int]],
                 configs: Dict[str, str] = None):
        self._app_name = app_name
        self._num_executors = num_executors
        self._executor_cores = executor_cores

        if isinstance(executor_memory, str):
            # If this is human readable str(like: 10KB, 10MB..), parse it
            executor_memory = parse_memory_size(executor_memory)

        self._executor_memory = executor_memory

        self._placement_group_strategy = placement_group_strategy
        self._placement_group = placement_group
        self._placement_group_bundle_indexes = placement_group_bundle_indexes

        self._configs = {} if configs is None else configs

        self._spark_cluster: Optional[SparkCluster] = None
        self._spark_session: Optional[SparkSession] = None

    def _get_or_create_spark_cluster(self) -> SparkCluster:
        if self._spark_cluster is not None:
            return self._spark_cluster
        self._spark_cluster = SparkCluster(self._configs)
        return self._spark_cluster

    def _prepare_placement_group(self):
        if self._placement_group_strategy is not None:
            bundles = []
            for _ in range(self._num_executors):
                bundles.append({"CPU": self._executor_cores,
                                "memory": self._executor_memory})
            pg = ray.util.placement_group(bundles, strategy=self._placement_group_strategy)
            ray.get(pg.ready())
            self._placement_group = pg
            self._placement_group_bundle_indexes = None

        if self._placement_group is not None:
            self._configs[self._PLACEMENT_GROUP_CONF] = self._placement_group.id.hex()
            bundle_indexes = list(range(self._placement_group.bundle_count)) \
                if self._placement_group_bundle_indexes is None \
                else self._placement_group_bundle_indexes
            self._configs[self._BUNDLE_INDEXES_CONF] = ",".join(map(str, bundle_indexes))

    def get_or_create_session(self):
        if self._spark_session is not None:
            return self._spark_session
        self.handle = RayDPConversionHelper.options(name=RAYDP_OBJ_HOLDER_NAME).remote()
        self._prepare_placement_group()
        spark_cluster = self._get_or_create_spark_cluster()
        self._spark_session = spark_cluster.get_spark_session(
            self._app_name,
            self._num_executors,
            self._executor_cores,
            self._executor_memory,
            self._configs)
        return self._spark_session

    def stop(self, del_obj_holder=True):
        if self._spark_session is not None:
            self._spark_session.stop()
            self._spark_session = None
        if self.handle is not None and del_obj_holder is True:
            self.handle.terminate.remote()
            self.handle = None
        if self._spark_cluster is not None:
            self._spark_cluster.stop()
            self._spark_cluster = None
        if self._placement_group_strategy is not None:
            if self._placement_group is not None:
                ray.util.remove_placement_group(self._placement_group)
                self._placement_group = None
            self._placement_group_strategy = None

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
               placement_group_strategy: Optional[str] = None,
               placement_group: Optional[PlacementGroup] = None,
               placement_group_bundle_indexes: Optional[List[int]] = None,
               configs: Optional[Dict[str, str]] = None):
    """
    Init a Spark cluster with given requirements.
    :param app_name: The application name.
    :param num_executors: number of executor requests
    :param executor_cores: the number of CPU cores for each executor
    :param executor_memory: the memory size for each executor, both support bytes or human
                            readable string.
    :param placement_group_strategy: RayDP will create a placement group according to the
                                     strategy and the configured resources for executors.
                                     If this parameter is specified, the next two
                                     parameters placement_group and
                                     placement_group_bundle_indexes will be ignored.
    :param placement_group: placement_group to schedule executors on
    :param placement_group_bundle_indexes: which bundles to use. If it's not specified,
                                           all bundles will be used.
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
                    app_name, num_executors, executor_cores, executor_memory,
                    placement_group_strategy,
                    placement_group,
                    placement_group_bundle_indexes,
                    configs)
                return _global_spark_context.get_or_create_session()
            except:
                _global_spark_context = None
                raise
        else:
            raise Exception("The spark environment has inited.")


def stop_spark(del_obj_holder=True):
    with _spark_context_lock:
        global _global_spark_context
        if _global_spark_context is not None:
            _global_spark_context.stop(del_obj_holder)
            if del_obj_holder is True:
                _global_spark_context = None


atexit.register(stop_spark)
