import logging

import pytest
from pyspark.sql import SparkSession
import ray


def quiet_logger():
    py4j_logger = logging.getLogger("py4j")
    py4j_logger.setLevel(logging.WARNING)

    koalas_logger = logging.getLogger("koalas")
    koalas_logger.setLevel(logging.WARNING)


@pytest.fixture(scope="function")
def spark_session(request):
    spark = SparkSession.builder.master("local[2]").appName("RayDP test").getOrCreate()
    request.addfinalizer(lambda: spark.stop())
    quiet_logger()
    return spark


@pytest.fixture(scope="function")
def ray_cluster(request):
    ray.init(include_java=True, redis_password="123")
    request.addfinalizer(lambda: ray.shutdown())
