from abc import abstractmethod
from typing import Dict

import pyspark

from raydp.services import Cluster
from raydp.spark.resource_manager.exchanger import SharedDataset

# This _global_cluster should be set after the cluster has created
_global_cluster = None


class SparkCluster(Cluster):
    """
    A abstract class to support save Spark DataFrame to ray object store.
    """

    @abstractmethod
    def get_spark_session(self,
                          app_name: str,
                          num_executors: int,
                          executor_cores: int,
                          executor_memory: int,
                          extra_conf: Dict[str, str] = None) -> pyspark.sql.SparkSession:
        pass

    @abstractmethod
    def save_to_ray(self, df: pyspark.sql.DataFrame) -> SharedDataset:
        pass
