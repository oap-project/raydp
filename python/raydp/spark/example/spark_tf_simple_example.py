import argparse
from typing import Dict

import pyspark
import ray
from pyspark.sql.functions import rand
from ray.util.sgd.tf.tf_trainer import TFTrainer

from raydp.spark.resource_manager.standalone.block_holder import ObjectIdList
from raydp.spark.resource_manager.standalone.standalone_cluster import save_to_ray, SparkCluster
from raydp.spark.tf.tf_dataset import create_dataset_from_objects

parser = argparse.ArgumentParser(description="A simple example for spark on ray")
parser.add_argument("--redis-address", type=str, dest="redis_address",
                    help="The ray redis address(host:port) when you want to connect to existed"
                         " ray cluster. Will startup a ray cluster if this is not provided")
parser.add_argument("--redis-password", type=str, dest="redis_password",
                    help="The ray redis password when you want to connect to existed ray "
                         "cluster. This must provide when connect to existed cluster.")

parser.add_argument("--spark-home", type=str, required=True, dest="spark_home",
                    help="The spark home directory")

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
# We can't hide the creation of SparkCluster easily because of we need to stop the cluster
# manually. However, this can be improved after we support startup spark natively.
spark_cluster = SparkCluster(spark_home=args.spark_home)
# get SparkSession
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
# TODO: hide this
ray_objects: ObjectIdList = save_to_ray(df)


# ---------------- ray sgd -------------------------
def create_mode(config: Dict):
    import tensorflow as tf
    model = tf.keras.Sequential()
    model.add(tf.keras.layers.Dense(1, input_shape=(2,)))
    model.compile(optimizer=tf.keras.optimizers.Adam(0.01),
                  loss=tf.keras.losses.mean_squared_error,
                  metrics=["accuracy", "mse"])
    return model


def data_creator(config: Dict):
    train_dataset = create_dataset_from_objects(
                        objs=ray_objects,
                        features_columns=["x", "y"],
                        label_column="z").repeat().batch(1000)
    test_dataset = None
    return train_dataset, test_dataset


tf_trainer = TFTrainer(model_creator=create_mode,
                       data_creator=data_creator,
                       num_replicas=2,
                       config={"fit_config": {"steps_per_epoch": 100}})

for i in range(100):
    stats = tf_trainer.train()
    print(f"Step: {i}, stats: {stats}")

model = tf_trainer.get_model()
print(model.get_weights())

tf_trainer.shutdown()

spark.stop()
spark_cluster.stop()

ray.shutdown()
