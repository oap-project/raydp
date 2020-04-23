import argparse

import pyspark
from pyspark.sql.functions import rand, round

import ray
from ray.util.sgd.tf.tf_trainer import TFTrainer

from spark_on_ray.spark.dataholder import ObjectIdList
from spark_on_ray.spark.spark_cluster import save_to_ray, SparkCluster, _global_data_holder
from spark_on_ray.spark.utils import create_dataset_from_objects

from typing import Dict, List


parser = argparse.ArgumentParser(description="A simple example for spark on ray")
parser.add_argument("--redis-address", type=str, dest="redis_address",
                    help="The ray redis address(host:port) when you want to connect to existed"
                         " ray cluster. Will startup a ray cluster if this is not provided")
parser.add_argument("--redis-password", type=str, dest="redis_password",
                    help="The ray redis password when you want to connect to existed ray "
                         "cluster. This must provide when connect to existed cluster.")

parser.add_argument("--spark-home", type=str, required=True, dest="spark_home",
                    help="The spark home directory")

parser.add_argument("--num-workers", type=int, dest="num_workers",
                    help="The number of standalone workers to start up, "
                         "this will be same as num-executors if it is not set")
parser.add_argument("--worker-cores", type=int, dest="worker_cores",
                    help="The number of cores for each of standalone worker, "
                         "this will be same as executor-cores if it is not set")
parser.add_argument("--worker-memory", type=float, dest="worker_memory",
                    help="The size of memory(GB) for each of standalone worker, "
                         "this will be same as executor-memory if it is not set")

parser.add_argument("--num-executors", type=int, required=True, dest="num_executors",
                    help="The number of executors for this application")
parser.add_argument("--executor-cores", type=int, required=True, dest="executor_cores",
                    help="The number of cores for each of Spark executor")
parser.add_argument("--executor-memory", type=float, required=True, dest="executor_memory",
                    help="The size of memory(GB) for each of Spark executor")

args = parser.parse_args()

GB = 1 * 1024 * 1024 * 1024

# -------------------- set up ray cluster --------------------
if args.redis_address:
    assert args.redis_password,\
        "Connect to existed cluster must provide both redis address and password"
    print("Connect to existed cluster.")
    ray.init(address=args.redis_address,
             node_ip_address=None,
             redis_password=args.redis_password)
else:
    print("Start up new cluster")
    ray.init()

# -------------------- setup spark -------------------------

# set up master node
spark_cluster = SparkCluster(spark_home=args.spark_home)

num_workers = args.num_workers if args.num_workers else args.num_executors
worker_cores = args.worker_cores if args.worker_cores else args.executor_cores
worker_memory = args.worker_memory if args.worker_memory else args.executor_memory
worker_resources = {"num_cpus": worker_cores,
                    "memory": int(worker_memory * GB)}


# add spark worker, using the same resource requirements as executor
for _ in range(num_workers):
    spark_cluster.add_worker(worker_resources)

# get SparkSession from spark cluster
spark = spark_cluster.get_spark_session(
    app_name="A simple example for spark on ray",
    num_executors=args.num_executors,
    executor_cores=args.executor_cores,
    executor_memory=int(args.executor_memory * GB))


# ---------------- data process with Spark ------------
# calculate z = 3 * x + 4 * y + 5
df: pyspark.sql.DataFrame = spark.range(0, 100000)
df = df.withColumn("x", rand() * 100)  # add x column
df = df.withColumn("y", rand() * 1000)  # ad y column
df = df.withColumn("z", df.x * 3 + df.y * 4 + rand() + 5)  # ad z column
df = df.select(df.x, df.y, df.z)

# save DataFrame to ray
ray_objects: ObjectIdList = save_to_ray(df)


# ---------------- ray sgd -------------------------
def create_mode(config: Dict):
    import tensorflow as tf
    model = tf.keras.Sequential()
    model.add(tf.keras.layers.Dense(1))
    model.compile(optimizer=tf.keras.optimizers.Adam(0.01),
                  loss=tf.keras.losses.mean_squared_error,
                  metrics=["accuracy", "mse"])
    return model


def data_creator(config: Dict):
    train_dataset = create_dataset_from_objects(
                        objs=ray_objects,
                        features_columns=["x", "y"],
                        label_column="z",
                        data_holder_mapping=_global_data_holder)
    test_dataset = None
    return train_dataset, test_dataset


tf_trainer = TFTrainer(model_creator=create_mode,
                       data_creator=data_creator,
                       num_replicas=2)

for i in range(100):
    stats = tf_trainer.train()
    print(f"Step: {i}, stats: {stats}")

model = tf_trainer.get_model()
print(model.summary())

tf_trainer.shutdown()

spark.stop()

spark_cluster.stop()
ray.shutdown()
