import argparse

import databricks.koalas as ks

import ray
from ray.util.sgd.torch.torch_trainer import TorchTrainer

from raydp.spark.dataholder import ObjectIdList
from raydp.spark.spark_cluster import save_to_ray, SparkCluster
from raydp.spark.torch_dataset import RayDataset

import torch

from typing import Dict

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
    assert args.redis_password, \
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


# ---------------- data process with koalas ------------
# calculate z = 3 * x + 4 * y + 5
df: ks.DataFrame = ks.range(0, 100000)
df["x"] = df["id"] + 100
df["y"] = df["id"] + 1000
df["z"] = df["x"] * 3 + df["y"] * 4 + 5

# save DataFrame to ray
# TODO: hide this
ray_objects: ObjectIdList = save_to_ray(df)


# ---------------- ray sgd -------------------------
def create_mode(config: Dict):
    model = torch.nn.Sequential(
        torch.nn.Linear(2, 1)
    )
    return model


def data_creator(config: Dict):
    dataset = RayDataset(ray_objects,
                         features_columns=["x", "y"],
                         label_column="z")
    dataloader = torch.utils.data.DataLoader(dataset, batch_size=1000)
    return dataloader, None


def optimizer_creator(model, config):
    return torch.optim.Adam(model.parameters())


trainer = TorchTrainer(model_creator=create_mode,
                       data_creator=data_creator,
                       optimizer_creator=optimizer_creator,
                       loss_creator=torch.nn.MSELoss,
                       num_workers=2)

for i in range(100):
    stats = trainer.train()
    print(f"Step: {i}, stats: {stats}")

model = trainer.get_model()
print(model.parameters())

trainer.shutdown()

spark.stop()
spark_cluster.stop()

ray.shutdown()
