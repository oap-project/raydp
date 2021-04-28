import argparse
import torch
import torch.nn as nn
import torch.nn.functional as F
from torch.utils.data.dataloader import DataLoader

import horovod.torch as hvd
import raydp
from raydp.spark import RayMLDataset

from data_process import nyc_taxi_preprocess, NYC_TRAIN_CSV

# Training settings
parser = argparse.ArgumentParser(description='Horovod NYC taxi Example')
parser.add_argument(
    '--batch-size',
    type=int,
    default=64,
    metavar='N',
    help='input batch size for training (default: 64)')
parser.add_argument(
    '--epochs',
    type=int,
    default=5,
    metavar='N',
    help='number of epochs to train (default: 10)')
parser.add_argument(
    '--lr',
    type=float,
    default=0.01,
    metavar='LR',
    help='learning rate (default: 0.01)')
parser.add_argument(
    '--log-interval',
    type=int,
    default=10,
    metavar='N',
    help='how many batches to wait before logging training status')

args = parser.parse_args()

class NYC_Model(nn.Module):
    def __init__(self, cols):
        super(NYC_Model, self).__init__()
        
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

def process_data():
    app_name = "NYC Taxi Fare Prediction with RayDP"
    num_executors = 1
    cores_per_executor = 1
    memory_per_executor = "500M"
    # Use RayDP to perform data processing
    spark = raydp.init_spark(app_name, num_executors, cores_per_executor, memory_per_executor)
    data = spark.read.format("csv").option("header", "true") \
            .option("inferSchema", "true") \
            .load(NYC_TRAIN_CSV)
    # Set spark timezone for processing datetime
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    data = nyc_taxi_preprocess(data)
    ds = RayMLDataset.from_spark(data, 1, args.batch_size)
    features = [field.name for field in list(data.schema) if field.name != "fare_amount"]
    return ds.to_torch(feature_columns=features, label_column="fare_amount"), len(features)

def train_fn(dataset, num_features):
    hvd.init()
    rank = hvd.rank()
    print(rank)
    train_data = dataset.get_shard(rank)
    train_loader = DataLoader(train_data, batch_size=args.batch_size)
    model = NYC_Model(num_features)
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
        for batch_idx, data in enumerate(train_loader):
            feature = data[:-1]
            target = data[-1]
            optimizer.zero_grad()
            output = model(*feature)
            loss = F.smooth_l1_loss(output, target)
            loss.backward()
            optimizer.step()
            if batch_idx % args.log_interval == 0:
                print('Train Epoch: {} \tLoss: {:.6f}'.format(
                    epoch, loss.item()))
    for epoch in range(1, args.epochs + 1):
        train(epoch)

if __name__ == '__main__':
    # connect to ray cluster
    import ray
    # ray.init(address='auto')
    ray.init()
    torch_ds, num_features = process_data()
    # Start horovod workers on Ray
    from horovod.ray import RayExecutor
    settings = RayExecutor.create_settings(500)
    executor = RayExecutor(settings, num_hosts=1, num_slots=1, cpus_per_slot=1)
    executor.start()
    executor.run(train_fn, args=[torch_ds, num_features])
    raydp.stop_spark()
    ray.shutdown()
