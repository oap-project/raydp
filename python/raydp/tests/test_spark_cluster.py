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
import sys
import time
import platform
import pytest
import pyarrow
import ray

from multiprocessing import get_context

from ray.util.placement_group import placement_group_table

import raydp
import raydp.utils as utils
from raydp.spark.ray_cluster_master import RayDPSparkMaster, RAYDP_SPARK_MASTER_SUFFIX
from ray.cluster_utils import Cluster


def test_spark(spark_on_ray_small):
    spark = spark_on_ray_small
    result = spark.range(0, 10).count()
    assert result == 10


def test_legacy_spark_on_fractional_cpu():
    cluster = Cluster(
        initialize_head=True,
        connect=True,
        head_node_args={
            "num_cpus": 2
        })

    spark = raydp.init_spark(app_name="test_cpu_fraction",
                             num_executors=1, executor_cores=3, executor_memory="500M",
                             configs={"spark.ray.actor.resource.cpu": "0.1"})
    result = spark.range(0, 10).count()
    assert result == 10

    spark.stop()
    raydp.stop_spark()
    time.sleep(5)
    ray.shutdown()
    cluster.shutdown()


def test_spark_on_fractional_cpu():
    cluster = Cluster(
        initialize_head=True,
        connect=True,
        head_node_args={
            "num_cpus": 2
        })

    spark = raydp.init_spark(app_name="test_cpu_fraction",
                             num_executors=1, executor_cores=3, executor_memory="500M",
                             configs={"spark.ray.raydp_spark_executor.actor.resource.cpu": "0.1"})
    result = spark.range(0, 10).count()
    assert result == 10

    spark.stop()
    raydp.stop_spark()
    time.sleep(5)
    ray.shutdown()
    cluster.shutdown()


def test_spark_executor_node_affinity():
    cluster = Cluster(
        initialize_head=True,
        connect=True,
        head_node_args={
            "num_cpus": 1,
        })
    cluster.add_node(num_cpus=2, resources={"spark_executor": 10})

    spark = raydp.init_spark(app_name="test_executor_node_affinity",
                             num_executors=1, executor_cores=2, executor_memory="500M",
                             configs={"spark.ray.raydp_spark_executor.actor.resource.spark_executor": "1"})
    result = spark.range(0, 10).count()
    assert result == 10

    raydp.stop_spark()
    time.sleep(5)
    ray.shutdown()
    cluster.shutdown()


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
            time.sleep(5)

    driver = SparkRemote.remote()
    result = ray.get(driver.run.remote())
    assert result == 100
    ray.get(driver.stop.remote())


def test_spark_driver_and_executor_hostname(spark_on_ray_small):
    if platform.system() == "Darwin":
        pytest.skip("Skip this test on mac")
    conf = spark_on_ray_small.conf
    node_ip_address = ray.util.get_node_ip_address()

    driver_host_name = conf.get("spark.driver.host")
    assert node_ip_address == driver_host_name
    driver_bind_address = conf.get("spark.driver.bindAddress")
    assert node_ip_address == driver_bind_address


def test_ray_dataset_roundtrip(spark_on_ray_2_executors):
    # skipping this to be compatible with ray 2.4.0
    # see issue #343
    if not ray.worker.global_worker.connected:
        pytest.skip("Skip this test if using ray client")
    spark = spark_on_ray_2_executors
    spark_df = spark.createDataFrame([(1, "a"), (2, "b"), (3, "c")], ["one", "two"])
    rows = [(r.one, r.two) for r in spark_df.take(3)]
    ds = ray.data.from_spark(spark_df)
    values = [(r["one"], r["two"]) for r in ds.take(6)]
    assert values == rows
    df = raydp.spark.dataset. \
        ray_dataset_to_spark_dataframe(spark, ds.schema(), ds.get_internal_block_refs())
    rows_2 = [(r.one, r.two) for r in df.take(3)]
    assert values == rows_2


def test_ray_dataset_to_spark(spark_on_ray_2_executors):
    # skipping this to be compatible with ray 2.4.0
    # see issue #343
    if not ray.worker.global_worker.connected:
        pytest.skip("Skip this test if using ray client")
    spark = spark_on_ray_2_executors
    n = 5
    data = {"value": list(range(n))}
    ds = ray.data.from_arrow(pyarrow.Table.from_pydict(data))
    values = [r["value"] for r in ds.take(n)]
    df = raydp.spark.dataset. \
        ray_dataset_to_spark_dataframe(spark, ds.schema(), ds.get_internal_block_refs())
    rows = [r.value for r in df.take(n)]
    assert values == rows
    ds2 = ray.data.from_items([{"id": i} for i in range(n)])
    ids = [r["id"] for r in ds2.take(n)]
    df2 = raydp.spark.dataset. \
        ray_dataset_to_spark_dataframe(spark, ds2.schema(), ds2.get_internal_block_refs())
    rows2 = [r.id for r in df2.take(n)]
    assert ids == rows2


def test_placement_group(ray_cluster):
    for pg_strategy in ["PACK", "STRICT_PACK", "SPREAD", "STRICT_SPREAD"]:
        spark = raydp.init_spark(f"test_strategy_{pg_strategy}_1", 1, 1, "500M",
                                 placement_group_strategy=pg_strategy)
        result = spark.range(0, 10, numPartitions=10).count()
        assert result == 10
        raydp.stop_spark()

        time.sleep(3)

        # w/ existing placement group w/ bundle indexes
        pg = ray.util.placement_group([{"CPU": 1, "memory": utils.parse_memory_size("500M")}],
                                      strategy=pg_strategy)
        ray.get(pg.ready())
        spark = raydp.init_spark(f"test_bundle_{pg_strategy}_2", 1, 1, "500M",
                                 placement_group=pg,
                                 placement_group_bundle_indexes=[0])
        result = spark.range(0, 10, numPartitions=10).count()
        assert result == 10
        raydp.stop_spark()

        time.sleep(5)

        # w/ existing placement group w/o bundle indexes
        spark = raydp.init_spark(f"test_bundle_{pg_strategy}_3", 1, 1, "500M",
                                 placement_group=pg)
        result = spark.range(0, 10, numPartitions=10).count()
        assert result == 10
        raydp.stop_spark()
        ray.util.remove_placement_group(pg)

        time.sleep(3)

    num_non_removed_pgs = len([
        p for pid, p in placement_group_table().items()
        if p["state"] != "REMOVED"
    ])
    assert num_non_removed_pgs == 0


def test_reconstruction():
    cluster = ray.cluster_utils.Cluster()
    # Head node has 2 cores for necessray actors
    head = cluster.add_node(
        num_cpus=2,
        include_dashboard=False,
        enable_object_reconstruction=True
    )
    ray.init(address=cluster.address, include_dashboard=False)
    # init_spark before adding nodes to ensure drivers connect to the head node
    spark = raydp.init_spark('a', 2, 1, '500m', fault_tolerant_mode=True)
    # Add two nodes, 1 executor each
    node_to_kill = cluster.add_node(num_cpus=1, include_dashboard=False, object_store_memory=10 ** 8)
    second_node = cluster.add_node(num_cpus=1, include_dashboard=False, object_store_memory=10 ** 8)
    # wait for executors to start
    time.sleep(5)
    # df should be large enough so that result will be put into plasma
    df = spark.range(100000)
    ds = raydp.spark.from_spark_recoverable(df)
    # remove the node, object get lost
    cluster.remove_node(node_to_kill)
    # add a node back, otherwise executor cannot restart due to lack of resource
    cluster.add_node(
        num_cpus=1, object_store_memory=10 ** 8
    )
    # verify that block is recovered
    for block in ds.get_internal_block_refs():
        ray.get(block)
    raydp.stop_spark()
    ray.shutdown()
    cluster.shutdown()


@pytest.mark.skip("flaky")
def test_custom_installed_spark(custom_spark_dir):
    os.environ["SPARK_HOME"] = custom_spark_dir
    cluster = Cluster(
        initialize_head=True,
        head_node_args={
            "num_cpus": 2
        })

    ray.init(address=cluster.address)
    app_name = "custom_install_test"
    spark = raydp.init_spark(app_name, 1, 1, "500M")
    spark_master_actor = ray.get_actor(name=app_name + RAYDP_SPARK_MASTER_SUFFIX)
    spark_home = ray.get(spark_master_actor.get_spark_home.remote())

    result = spark.range(0, 10).count()
    raydp.stop_spark()
    ray.shutdown()

    assert result == 10
    assert spark_home == custom_spark_dir


def start_spark(barrier, i, results):
    try:
        # connect to the cluster started before pytest
        ray.init(address="auto")
        spark = raydp.init_spark(f"spark-{i}", 1, 1, "500M")
        # wait on barrier to ensure 2 spark sessions
        # are active on the same ray cluster at the same time
        barrier.wait()
        df = spark.range(10)
        results[i] = df.count()
        raydp.stop_spark()
        ray.shutdown()
    except Exception as e:
        results[i] = -1


def test_init_spark_twice():
    num_processes = 2
    ctx = get_context("spawn")
    barrier = ctx.Barrier(num_processes)
    # shared memory for processes to return if spark started successfully
    results = ctx.Array('i', [-1] * num_processes)
    processes = [ctx.Process(target=start_spark, args=(barrier, i, results)) for i in range(num_processes)]
    for i in range(2):
        processes[i].start()

    for i in range(2):
        processes[i].join()

    assert results[0] == 10
    assert results[1] == 10


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
