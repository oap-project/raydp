from spark_on_ray.spark.dataholder import ObjectIdWrapper
from spark_on_ray.spark.spark_cluster import save_to_ray, SparkCluster

import pyspark
import ray

from typing import List


GB = 1 * 1024 * 1024 * 1024

# ------------- connect to ray cluster ------------
redis_address = "192.168.1.17:23960"
redis_password = "123"
ray.init(address=redis_address, redis_password=redis_password)

# -------------- setup spark cluster ----------------
master_resources = {"num_cpus": 1}
spark_home = "/Users/xianyang/opt/spark-3.0.0-preview2-bin-hadoop2.7"
# set up master node
spark_cluster = SparkCluster(
    ray_redis_address=redis_address,
    ray_redis_password=redis_password,
    master_resources=master_resources,
    spark_home=spark_home)

# add spark worker
worker_resources = {"num_cpus": 2, "memory": 1 * GB}
spark_cluster.add_worker(worker_resources)

# --------- data process with Spark ------------
# get SparkSession from spark cluster
spark: pyspark.sql.SparkSession = spark_cluster.get_spark_session(
                                      app_name="SimpleTest",
                                      num_executors=1,
                                      executor_cores=1,
                                      executor_memory="512m")

df: pyspark.sql.DataFrame = spark.range(0, 10)

# save DataFrame to ray
ray_objects: List[ObjectIdWrapper] = save_to_ray(df)

for obj in ray_objects:
    d = obj.get()
    print(type(d))
    print(d)

spark.stop()

spark_cluster.stop()
ray.shutdown()
