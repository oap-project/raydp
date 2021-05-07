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

import os
import subprocess
import sys

import pytest
import ray
import pyspark

import raydp


def test_spark(spark_on_ray_small):
    spark = spark_on_ray_small
    result = spark.range(0, 10).count()
    assert result == 10


def test_spark_remote(ray_cluster):
    @ray.remote
    class SparkRemote:
        def __init__(self):
            self.spark = raydp.init_spark(app_name="test_spark_remote",
                                          num_executors=1,
                                          executor_cores=1,
                                          executor_memory="500MB")

        def run(self):
            return self.spark.range(0, 100).count()

        def stop(self):
            self.spark.stop()
            raydp.stop_spark()

    driver = SparkRemote.remote()
    result = ray.get(driver.run.remote())
    assert result == 100
    ray.get(driver.stop.remote())


def test_spark_driver_and_executor_hostname(spark_on_ray_small):
    conf = spark_on_ray_small.conf
    node_ip_address = ray.services.get_node_ip_address()

    driver_host_name = conf.get("spark.driver.host")
    assert node_ip_address == driver_host_name
    driver_bind_address = conf.get("spark.driver.bindAddress")
    assert node_ip_address == driver_bind_address

def test_spark_submit(spark_on_ray_small):
    conf = spark_on_ray_small.conf
    master = conf.get('spark.master')
    raydp_dir = os.path.dirname(raydp.__file__)
    command = [raydp_dir + '/bin/raydp-submit', '--master', master]
    command += ['--conf', 'spark.executor.cores=1']
    command += ['--conf', 'spark.executor.instances=1']
    pyspark_dir = os.path.dirname(pyspark.__file__)
    command += [pyspark_dir + "/examples/src/main/python/pi.py"]
    subprocess.run(command, check=True)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
