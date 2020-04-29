from contextlib import ContextDecorator

import os

from raydp.spark.spark_cluster import SparkCluster

from typing import Dict, Optional


class spark_context(ContextDecorator):
    def __init__(self,
                 app_name: str,
                 num_executors: int,
                 executor_cores: int,
                 executor_memory: Optional[int, str],
                 spark_home: str = None,
                 configs: Dict[str, str] = {}):
        if spark_home is None:
            # find spark home from environment
            if "SPARK_HOME" not in os.environ:
                raise Exception(
                    "Spark home must be set or set it in environment with key 'SPARK_HOME'")
            else:
                spark_home = os.environ["SPARK_HOME"]

        self._app_name = app_name
        self._spark_home = spark_home
        self._num_executors = num_executors
        self._executor_cores = executor_cores
        self._executor_memory = executor_memory
        self._configs = configs

        self._spark_cluster: SparkCluster = None
        self._spark_session = None

    def __enter__(self):
        # create spark cluster
        self._spark_cluster = SparkCluster(self._spark_home)
        self._spark_session = self._spark_cluster.get_spark_session(
            self._app_name,
            self._num_executors,
            self._executor_cores,
            self._executor_memory,
            **self._configs)

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._spark_session is not None:
            self._spark_session.close()
            self._spark_session = None
        if self._spark_cluster is not None:
            self._spark_cluster.stop()
            self._spark_cluster = None


