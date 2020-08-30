import argparse

import pyspark
import ray
import tensorflow.keras as keras
from pyspark.sql.functions import rand

from raydp.spark import context
from raydp.spark.utils import random_split
from raydp.spark.tf.estimator import TFEstimator

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
parser.add_argument("--executor-memory", type=str, required=True, dest="executor_memory",
                    help="The size of memory for each of Spark executor")

args = parser.parse_args()

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

app_name = "A simple example for spark on ray"
num_executors = args.num_executors
executor_cores = args.executor_cores
executor_memory = args.executor_memory

spark = context.init_spark(app_name, num_executors, executor_cores, executor_memory)

# ---------------- data process with Spark ------------
# calculate z = 3 * x + 4 * y + 5
df: pyspark.sql.DataFrame = spark.range(0, 100000)
df = df.withColumn("x", rand() * 100)  # add x column
df = df.withColumn("y", rand() * 1000)  # ad y column
df = df.withColumn("z", df.x * 3 + df.y * 4 + rand() + 5)  # ad z column
df = df.select(df.x, df.y, df.z)

train_df, test_df = random_split(df, [0.7, 0.3])


# ---------------- ray sgd -------------------------
# create model
input_1 = keras.Input(shape=(1,))
input_2 = keras.Input(shape=(1,))

concatenated = keras.layers.concatenate([input_1, input_2])
output = keras.layers.Dense(1, activation='sigmoid')(concatenated)
model = keras.Model(inputs=[input_1, input_2],
                    outputs=output)

optimizer = keras.optimizers.Adam(0.01)
loss = keras.losses.MeanSquaredError()

estimator = TFEstimator(num_workers=2,
                        model=model,
                        optimizer=optimizer,
                        loss=loss,
                        metrics=["accuracy", "mse"],
                        feature_columns=["x", "y"],
                        label_column="z",
                        batch_size=1000,
                        num_epochs=2,
                        config={"fit_config": {"steps_per_epoch": 100}})

estimator.fit(train_df)
estimator.evaluate(test_df)

estimator.shutdown()
context.stop_spark()
ray.shutdown()