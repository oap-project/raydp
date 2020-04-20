from spark_on_ray.spark.spark_cluster import SparkCluster
from spark_on_ray.spark.utils import load_into_ids, save_to_ray

import ray


GB = 1 * 1024 * 1024 * 1024

# connect ray to cluster
redis_address = "192.168.1.17:46846"
redis_password = "123"
ray.init(node_ip_address="192.168.1.17", redis_password=redis_password)

# setup spark master
master_resources = {"num_cpus": 1}
spark_home = "/Users/xianyang/opt/spark-2.4.5-bin-hadoop2.7"
spark_cluster = SparkCluster(
    ray_redis_address=redis_address,
    ray_redis_password=redis_password,
    master_resources=master_resources,
    spark_home=spark_home)

# add spark worker
worker_resources = {"num_cpus": 2, "memory": 1 * GB}
spark_cluster.add_worker(worker_resources)

# get spark session from spark cluster
spark = spark_cluster.get_spark_session(app_name="SimpleTest",
                                        num_executors=1,
                                        executor_cores=1,
                                        executor_memory="512m")

df = spark.range(0, 10)

ray_object_ids_in_bytes = df.select(save_to_ray(*df.columns)).collect()

ray_object_ids = load_into_ids(ray_object_ids_in_bytes)
print(ray.get(ray_object_ids))


spark.stop()

spark_cluster.stop()
ray.shutdown()
