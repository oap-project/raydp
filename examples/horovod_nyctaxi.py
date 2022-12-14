import argparse
import torch
import torch.nn as nn
import torch.nn.functional as F

import horovod.torch as hvd
import raydp
from ray.train.horovod import HorovodTrainer
from ray.air import session
from ray.air.config import ScalingConfig
from data_process import nyc_taxi_preprocess, NYC_TRAIN_CSV

# Training settings
parser = argparse.ArgumentParser(description="Horovod NYC taxi Example")
parser.add_argument(
    "--batch-size",
    type=int,
    default=64,
    metavar="N",
    help="input batch size for training (default: 64)")
parser.add_argument(
    "--epochs",
    type=int,
    default=5,
    metavar="N",
    help="number of epochs to train (default: 5)")
parser.add_argument(
    "--num-workers",
    type=int,
    default=2,
    metavar="N",
    help="number of workers to train (default: 1)")
parser.add_argument(
    "--lr",
    type=float,
    default=0.01,
    metavar="LR",
    help="learning rate (default: 0.01)")
parser.add_argument(
    "--log-interval",
    type=int,
    default=10,
    metavar="N",
    help="how many batches to wait before logging training status")

args = parser.parse_args()

class NYC_Model(nn.Module):
    def __init__(self, cols):
        super().__init__()
        self.fc1 = nn.Linear(cols, 256)
        self.fc2 = nn.Linear(256, 128)
        self.fc3 = nn.Linear(128, 64)
        self.fc4 = nn.Linear(64, 16)
        self.fc5 = nn.Linear(16, 1)
        self.bn1 = nn.BatchNorm1d(256)
        self.bn2 = nn.BatchNorm1d(128)
        self.bn3 = nn.BatchNorm1d(64)
        self.bn4 = nn.BatchNorm1d(16)

    def forward(self, *x):
        x = torch.cat(x, dim=1)
        x = F.relu(self.fc1(x))
        x = self.bn1(x)
        x = F.relu(self.fc2(x))
        x = self.bn2(x)
        x = F.relu(self.fc3(x))
        x = self.bn3(x)
        x = F.relu(self.fc4(x))
        x = self.bn4(x)
        x = self.fc5(x)
        return x

def process_data(num_workers):
    app_name = "NYC Taxi Fare Prediction with RayDP"
    num_executors = 1
    cores_per_executor = 1
    memory_per_executor = "1g"
    # Use RayDP to perform data processing
    spark = raydp.init_spark(app_name, num_executors, cores_per_executor, memory_per_executor)
    data = spark.read.format("csv").option("header", "true") \
            .option("inferSchema", "true") \
            .load(NYC_TRAIN_CSV)
    # Set spark timezone for processing datetime
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    data = nyc_taxi_preprocess(data)
    ds = ray.data.from_spark(data, parallelism=num_workers)
    features = [field.name for field in list(data.schema) if field.name != "fare_amount"]
    return ds, features

def train_fn(config):
    hvd.init()
    features = config.get("features")
    rank = hvd.rank()
    train_data_shard = session.get_dataset_shard("train")
    train_data =train_data_shard.to_torch(feature_columns=features,
                                          label_column="fare_amount",
                                          label_column_dtype=torch.float,
                                          feature_column_dtypes=torch.float,
                                          batch_size=args.batch_size)
    model = NYC_Model(len(features))
    lr_scaler = hvd.size()
    optimizer = torch.optim.Adam(model.parameters(), lr=lr_scaler * args.lr)
    # Horovod: broadcast parameters & optimizer state.
    hvd.broadcast_parameters(model.state_dict(), root_rank=0)
    hvd.broadcast_optimizer_state(optimizer, root_rank=0)
    # Horovod: wrap optimizer with DistributedOptimizer.
    optimizer = hvd.DistributedOptimizer(optimizer,
                                         named_parameters=model.named_parameters(),
                                         op=hvd.Average)
    def train(epoch):
        model.train()
        for batch_idx, (feature, target) in enumerate(train_data):
            optimizer.zero_grad()
            output = model(feature)
            loss = F.smooth_l1_loss(output, target)
            loss.backward()
            optimizer.step()
            if batch_idx % args.log_interval == 0:
                print("Train Epoch: {} \tLoss: {:.6f}".format(
                    epoch, loss.item()))
    for epoch in range(1, args.epochs + 1):
        train(epoch)

if __name__ == "__main__":
    # connect to ray cluster
    import ray
    ray.init(address="local", num_cpus=4)
    ds, features = process_data(args.num_workers)
    trainer = HorovodTrainer(train_loop_per_worker=train_fn,
                             train_loop_config={"features": features},
                             scaling_config=ScalingConfig(num_workers=args.num_workers),
                             datasets={"train": ds})
    trainer.fit()
    raydp.stop_spark()
    ray.shutdown()
