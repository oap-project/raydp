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

import logging

import pytest
from pyspark.sql import SparkSession
import ray
import raydp
import subprocess


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


@pytest.fixture(scope="function", params=["localhost:6379", "ray://localhost:10001"])
def ray_cluster(request):
    ray.shutdown()
    ray.init(address=request.param)
    request.addfinalizer(lambda: ray.shutdown())


@pytest.fixture(scope="function", params=["localhost:6379", "ray://localhost:10001"])
def spark_on_ray_small(request):
    ray.shutdown()
    ray.init(address=request.param)
    spark = raydp.init_spark("test", 1, 1, "500 M")

    def stop_all():
        raydp.stop_spark()
        ray.shutdown()

    request.addfinalizer(stop_all)
    return spark


@pytest.fixture(scope='module')
def custom_spark_dir(tmp_path_factory) -> str:
    working_dir = tmp_path_factory.mktemp("spark").as_posix()
    spark_distribution = 'spark-3.2.1-bin-hadoop3.2'
    file_extension = 'tgz'
    spark_distribution_file = f"{working_dir}/{spark_distribution}.{file_extension}"

    import wget

    wget.download(f"https://dlcdn.apache.org/spark/spark-3.2.1/{spark_distribution}.{file_extension}", spark_distribution_file)
    subprocess.check_output(['tar', 'xzvf', spark_distribution_file, '--directory', working_dir])
    return f"{working_dir}/{spark_distribution}"
