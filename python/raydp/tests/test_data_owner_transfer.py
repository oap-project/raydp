
import sys
import os
import time

import pytest
import ray
import raydp


def gen_test_data():
  from pyspark.sql.session import SparkSession
  s = SparkSession.getActiveSession()
 
  data = []
  tmp = [("ming", 20, 15552211521),
          ("hong", 19, 13287994007),
          ("dave", 21, 15552211523),
          ("john", 40, 15322211523),
          ("wong", 50, 15122211523)]

  for _ in range(10):
    data += tmp

  rdd = s.sparkContext.parallelize(data)
  out = s.createDataFrame(rdd, ["Name", "Age", "Phone"])
  return out
  

@pytest.mark.xfail(reason="data ownership transfer feature is not enabled")
def test_fail_without_data_ownership_transfer():
  """
  Test shutting down Spark worker after data been put 
  into Ray object store without data ownership transfer.
  This test should be throw error of data inaccessible after
  its owner (e.g. Spark JVM process) has terminated, which is expected.
  """

  from raydp.spark.dataset import spark_dataframe_to_ray_dataset
  
  num_executor = 1

  ray.shutdown()
  raydp.stop_spark()

  ray.init()
  spark = raydp.init_spark(
    app_name = "example",
    num_executors = num_executor,
    executor_cores = 1,
    executor_memory = "500M"
    )

  df_train = gen_test_data()
  # df_train = df_train.sample(False, 0.001, 42)

  resource_stats = ray.available_resources()
  cpu_cnt = resource_stats['CPU']

  # convert data from spark dataframe to ray dataset without data ownership transfer
  ds = spark_dataframe_to_ray_dataset(df_train, parallelism=4)

  # display data
  ds.show(5)

  # release resource by shutting down spark
  raydp.stop_spark()
  ray.internal.internal_api.global_gc() # ensure GC kicked in
  time.sleep(3)

  # confirm that resources has been recycled
  resource_stats = ray.available_resources()
  assert resource_stats['CPU'] == cpu_cnt + num_executor

  # confirm that data get lost (error thrown)
  ds.mean('Age')


def test_data_ownership_transfer():
  """
  Test shutting down Spark worker after data been put 
  into Ray object store with data ownership transfer.
  This test should be able to execute till the end without crash as expected.
  """

  from raydp.spark.dataset import spark_dataframe_to_ray_dataset
  import numpy as np
  
  num_executor = 1

  ray.shutdown()
  raydp.stop_spark()

  ray.init()
  spark = raydp.init_spark(
    app_name = "example",
    num_executors = num_executor,
    executor_cores = 1,
    executor_memory = "500M"
    )

  df_train = gen_test_data()

  resource_stats = ray.available_resources()
  cpu_cnt = resource_stats['CPU']

  # convert data from spark dataframe to ray dataset,
  # and transfer data ownership to dedicated Object Holder (Singleton)
  ds = spark_dataframe_to_ray_dataset(df_train, parallelism=4, _use_owner=True)

  # display data
  ds.show(5)

  # release resource by shutting down spark Java process
  raydp.stop_spark(del_obj_holder=False)
  ray.internal.internal_api.global_gc() # ensure GC kicked in
  time.sleep(3)

  # confirm that resources has been recycled
  resource_stats = ray.available_resources()
  assert resource_stats['CPU'] == cpu_cnt + num_executor

  # confirm that data is still available from object store!
  # sanity check the dataset is as functional as normal
  assert np.isnan(ds.mean('Age')) is not True
   
  # final clean up
  raydp.stop_spark()
  ray.shutdown()


def test_api_compatibility():
  """
  Test the changes been made are not to break public APIs.
  """

  num_executor = 1

  ray.shutdown()
  raydp.stop_spark()

  ray.init()
  spark = raydp.init_spark(
    app_name = "example",
    num_executors = num_executor,
    executor_cores = 1,
    executor_memory = "500M"
    )

  df_train = gen_test_data()

  resource_stats = ray.available_resources()
  cpu_cnt = resource_stats['CPU']

  # check compatibility of ray 1.9.0 API: no data onwership transfer
  ds = ray.data.from_spark(df_train)
  ray.internal.internal_api.global_gc() # ensure GC kicked in
  time.sleep(3)

  # confirm that resources is still being occupied
  resource_stats = ray.available_resources()
  assert resource_stats['CPU'] == cpu_cnt
  
  # final clean up
  raydp.stop_spark()
  ray.shutdown()


if __name__ == '__main__':
  sys.exit(pytest.main(["-v", __file__]))
  
  # test_api_compatibility()
  # test_data_ownership_transfer()
  # test_fail_without_data_ownership_transfer()

