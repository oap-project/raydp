import os
from contextlib import ContextDecorator
from threading import RLock
from typing import Dict, Union, Optional

from pyspark.sql import DataFrame
from pyspark.sql import SparkSession

from raydp.spark.utils import convert_to_spark, parse_memory_size
from raydp.spark.resource_manager.exchanger import SharedDataset
from raydp.spark.resource_manager.ray.ray_cluster import RayCluster
from raydp.spark.resource_manager.spark_cluster import SparkCluster
from raydp.spark.resource_manager.standalone.standalone_cluster import StandaloneCluster


SUPPORTED_RESOURCE_MANAGER = ("ray", "standalone")


class _spark_context(ContextDecorator):
    """
    A class used to create the Spark cluster and get the Spark session.

    .. code-block:: python

        @_spark_context(app_name, num_executors, executor_cores, executor_memory):
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
                 executor_memory: Union[str, int],
                 resource_manager: str = "ray",
                 spark_home: str = None,
                 configs: Dict[str, str] = None):
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

        if isinstance(executor_memory, str):
            # If this is human readable str(like: 10KB, 10MB..), parse it
            executor_memory = parse_memory_size(executor_memory)

        self._executor_memory = executor_memory
        self._configs = {} if configs is None else configs

        self._spark_cluster: Optional[SparkCluster] = None
        self._spark_session: Optional[SparkSession] = None

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
_global_spark_context: _spark_context = None


def init_spark(app_name: str,
               num_executors: int,
               executor_cores: int,
               executor_memory: Union[str, int],
               resource_manager: str = "ray",
               spark_home: Optional[str] = None,
               configs: Optional[Dict[str, str]] = None):
    """
    Init a Spark cluster with given requirements.
    :param app_name: The application name.
    :param num_executors: number of executor requests
    :param executor_cores: the number of CPU cores for each executor
    :param executor_memory: the memory size for each executor, both support bytes or human
                            readable string.
    :param resource_manager: this indicates how the cluster startup. We support standalone and
                             ray currently.
    :param spark_home: the spark home path, this is need when you choose standalone resource
                       manager
    :param configs: the extra Spark config need to set
    :return: return the SparkSession
    """
    with _spark_context_lock:
        global _global_spark_context
        if _global_spark_context is None:
            _global_spark_context = _spark_context(
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


def save_to_ray(df: Union[DataFrame, 'koalas.DataFrame']) -> SharedDataset:
    """
    Save the pyspark.sql.DataFrame or koalas.DataFrame to Ray ObjectStore and return
    a SharedDataset which could fit into the 'Estimator' for distributed model training.
    :param df: ether pyspark.sql.DataFrame or koalas.DataFrame
    :return: a SharedDataset
    """
    with _spark_context_lock:
        global _global_spark_context
        if _global_spark_context is None:
            raise Exception("You should init the Spark context firstly.")
        # convert to Spark sql DF
        df, is_df = convert_to_spark(df)
        return _global_spark_context._get_spark_cluster().save_to_ray(df)
