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
import pyspark
from pyspark.sql import SparkSession
import ray
import raydp
import subprocess
from ray.cluster_utils import Cluster


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


@pytest.fixture(scope="function", params=["local", "ray://localhost:10001"])
def ray_cluster(request):
    ray.shutdown()
    if request.param == "local":
        ray.init(address="local", num_cpus=6, include_dashboard=False)
    else:
        ray.init(address=request.param)
    request.addfinalizer(lambda: ray.shutdown())


@pytest.fixture(scope="function", params=["local", "ray://localhost:10001"])
def spark_on_ray_small(request):
    ray.shutdown()
    if request.param == "local":
        ray.init(address="local", num_cpus=6, include_dashboard=False)
    else:
        ray.init(address=request.param)
    node_ip = ray.util.get_node_ip_address()
    spark = raydp.init_spark("test", 1, 1, "500M", configs={
        "spark.driver.host": node_ip,
        "spark.driver.bindAddress": node_ip
    })

    def stop_all():
        raydp.stop_spark()
        ray.shutdown()

    request.addfinalizer(stop_all)
    return spark


@pytest.fixture(scope="function", params=["local", "ray://localhost:10001"])
def spark_on_ray_2_executors(request):
    ray.shutdown()
    if request.param == "local":
        ray.init(address="local", num_cpus=6, include_dashboard=False)
    else:
        ray.init(address=request.param)
    node_ip = ray.util.get_node_ip_address()
    spark = raydp.init_spark("test", 2, 1, "500M", configs={
        "spark.driver.host": node_ip,
        "spark.driver.bindAddress": node_ip
    })

    def stop_all():
        raydp.stop_spark()
        ray.shutdown()

    request.addfinalizer(stop_all)
    return spark


@pytest.fixture(scope="function")
def spark_on_ray_fraction_custom_resource(request):
    ray.shutdown()
    cluster = Cluster(
        initialize_head=True,
        head_node_args={
            "num_cpus": 2
        })
    ray.init(address=cluster.address)

    def stop_all():
        raydp.stop_spark()
        ray.shutdown()

    request.addfinalizer(stop_all)


@pytest.fixture(scope="function")
def spark_on_ray_fractional_cpu(request):
    ray.shutdown()
    cluster = Cluster(
        initialize_head=True,
        head_node_args={
            "num_cpus": 2
        })

    ray.init(address=cluster.address)

    spark = raydp.init_spark(app_name="test_cpu_fraction",
                             num_executors=1, executor_cores=3, executor_memory="500M",
                             configs={"spark.ray.actor.resource.cpu": "0.1"})

    def stop_all():
        raydp.stop_spark()
        ray.shutdown()

    request.addfinalizer(stop_all)
    return spark


@pytest.fixture(scope="function")
def spark_on_ray_executor_node_affinity(request):
    ray.shutdown()
    cluster = Cluster(
        initialize_head=True,
        head_node_args={
            "num_cpus": 2,
        })
    cluster.add_node(num_cpus=10, resources={"spark_executor": 10})

    ray.init(address=cluster.address)

    def stop_all():
        raydp.stop_spark()
        ray.shutdown()

    request.addfinalizer(stop_all)
    return cluster


@pytest.fixture(scope='session')
def custom_spark_dir(tmp_path_factory) -> str:
    working_dir = tmp_path_factory.mktemp("spark").as_posix()

    # Leave the if more verbose just in case the distribution name changed in the future.
    # Please make sure the version here is not the most recent release, so the file is available
    # in the archive download. Latest release's download URL (https://dlcdn.apache.org/spark/*)
    # will be changed to archive when the next release come out and break the test.
    if pyspark.__version__ == "3.2.1":
        spark_distribution = 'spark-3.2.1-bin-hadoop3.2'
    elif pyspark.__version__ == "3.1.3":
        spark_distribution = 'spark-3.1.3-bin-hadoop3.2'
    else:
        raise Exception(f"Unsupported Spark version {pyspark.__version__}.")

    file_extension = 'tgz'
    spark_distribution_file = f"{working_dir}/{spark_distribution}.{file_extension}"

    import wget

    wget.download(
        f"https://archive.apache.org/dist/spark/spark-{pyspark.__version__}/{spark_distribution}.{file_extension}",
        spark_distribution_file)
    subprocess.check_output(['tar', 'xzvf', spark_distribution_file, '--directory', working_dir])
    return f"{working_dir}/{spark_distribution}"
