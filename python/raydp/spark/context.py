import os
from contextlib import ContextDecorator
from threading import RLock
from typing import Dict

import pyspark

from raydp.spark.resource_manager.spark_cluster import SparkCluster
from raydp.spark.resource_manager.ray.ray_cluster import RayCluster
from raydp.spark.resource_manager.standalone.standalone_cluster import StandaloneCluster
from raydp.spark.resource_manager.exchanger import SharedDataset

SUPPORTED_RESOURCE_MANAGER = ("ray", "standalone")


class spark_context(ContextDecorator):
    """
    A class used to get the spark session.

    .. code-block:: python

        @spark_context(app_name, num_executors, executor_cores, executor_memory):
        def process():
            # you can code here just like the normal spark code
            spark = SparkSession.builder.getOrCreate()
            df = spark.read.parquet(...)
            ....

    """
    def __init__(self,
                 app_name: str,
                 num_executors: int,
                 executor_cores: int,
                 executor_memory: int,
                 resource_manager: str = "ray",
                 spark_home: str = None,
                 configs: Dict[str, str] = {}):
        if resource_manager.lower() not in SUPPORTED_RESOURCE_MANAGER:
            raise Exception(f"{resource_manager} is not supported")
        resource_manager = resource_manager.lower()
        if resource_manager == "standalone":
            # we need spark home if running on standalone
            if spark_home is None:
                # find spark home from environment
                if "SPARK_HOME" not in os.environ:
                    raise Exception(
                        "Spark home must be set or set it in environment with key 'SPARK_HOME'")
                else:
                    spark_home = os.environ["SPARK_HOME"]

        self._resource_manager = resource_manager
        self._app_name = app_name
        self._spark_home = spark_home
        self._num_executors = num_executors
        self._executor_cores = executor_cores
        self._executor_memory = executor_memory
        self._configs = configs

        self._spark_cluster: SparkCluster = None
        self._spark_session = None

    def _get_spark_cluster(self) -> SparkCluster:
        if self._spark_cluster is not None:
            return self._spark_cluster
        # create spark cluster
        if self._resource_manager == "ray":
            self._spark_cluster = RayCluster()
        elif self._resource_manager == "standalone":
            self._spark_cluster = StandaloneCluster(self._spark_home)
        return self._spark_cluster

    def _get_session(self):
        if self._spark_session is not None:
            return self._spark_session
        self._get_spark_cluster()
        self._spark_session = self._spark_cluster.get_spark_session(
            self._app_name,
            self._num_executors,
            self._executor_cores,
            self._executor_memory,
            self._configs)
        return self._spark_session

    def _stop(self):
        if self._spark_session is not None:
            self._spark_session.stop()
            self._spark_session = None
        if self._spark_cluster is not None:
            self._spark_cluster.stop()
            self._spark_cluster = None

    def __enter__(self):
        self._get_session()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._stop()


_spark_context_lock = RLock()
_global_spark_context: spark_context = None


def init_spark(app_name: str,
               num_executors: int,
               executor_cores: int,
               executor_memory: int,
               resource_manager: str = "ray",
               spark_home: str = None,
               configs: Dict[str, str] = {}):
    with _spark_context_lock:
        global _global_spark_context
        if _global_spark_context is None:
            _global_spark_context = spark_context(
                app_name, num_executors, executor_cores, executor_memory,
                resource_manager, spark_home, configs)
            return _global_spark_context._get_session()
        else:
            raise Exception("The spark environment has inited.")


def stop_spark():
    with _spark_context_lock:
        global _global_spark_context
        if _global_spark_context is not None:
            _global_spark_context._stop()
            _global_spark_context = None


def save_to_ray(df: pyspark.sql.DataFrame) -> SharedDataset:
    with _spark_context_lock:
        global _global_spark_context
        if _global_spark_context is None:
            raise Exception("You should init the Spark context firstly.")
        _global_spark_context._get_spark_cluster.save_to_ray(df)
