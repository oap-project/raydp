import argparse
import pandas as pd, numpy as np
import torch
import torch.nn as nn
import torch.nn.functional as F
from torch.utils.data.dataloader import DataLoader
from pyspark.sql.functions import *
import horovod.torch as hvd
import raydp
from raydp.spark import create_ml_dataset_from_spark
from raydp.utils import random_split

# Training settings
parser = argparse.ArgumentParser(description='PyTorch MNIST Example')
parser.add_argument(
    '--batch-size',
    type=int,
    default=64,
    metavar='N',
    help='input batch size for training (default: 64)')
parser.add_argument(
    '--test-batch-size',
    type=int,
    default=1000,
    metavar='N',
    help='input batch size for testing (default: 1000)')
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
    '--momentum',
    type=float,
    default=0.5,
    metavar='M',
    help='SGD momentum (default: 0.5)')
parser.add_argument(
    '--no-cuda',
    action='store_true',
    default=False,
    help='disables CUDA training')
parser.add_argument(
    '--seed',
    type=int,
    default=42,
    metavar='S',
    help='random seed (default: 42)')
parser.add_argument(
    '--log-interval',
    type=int,
    default=10,
    metavar='N',
    help='how many batches to wait before logging training status')
parser.add_argument(
    '--fp16-allreduce',
    action='store_true',
    default=False,
    help='use fp16 compression during allreduce')
parser.add_argument(
    '--use-adasum',
    action='store_true',
    default=False,
    help='use adasum algorithm to do reduction')
parser.add_argument(
    '--num-batches-per-commit',
    type=int,
    default=1,
    help='number of batches per commit of the elastic state object'
)
parser.add_argument(
    '--data-dir',
    help='location of the training dataset in the local filesystem (will be downloaded if needed)'
)

args = parser.parse_args()

def clean_up(data):
    
    data = data.filter(col('pickup_longitude')<=-72) \
            .filter(col('pickup_longitude')>=-76) \
            .filter(col('dropoff_longitude')<=-72) \
            .filter(col('dropoff_longitude')>=-76) \
            .filter(col('pickup_latitude')<=42) \
            .filter(col('pickup_latitude')>=38) \
            .filter(col('dropoff_latitude')<=42) \
            .filter(col('dropoff_latitude')>=38) \
            .filter(col('passenger_count')<=6) \
            .filter(col('passenger_count')>=1) \
            .filter(col('fare_amount') > 0) \
            .filter(col('fare_amount') < 250) \
            .filter(col('dropoff_longitude') != col('pickup_longitude')) \
            .filter(col('dropoff_latitude') != col('pickup_latitude')) 
    
    return data

# Add time related features
def add_time_features(data):
    
    data = data.withColumn("day", dayofmonth(col("pickup_datetime")))
    data = data.withColumn("hour_of_day", hour(col("pickup_datetime")))
    data = data.withColumn("day_of_week", dayofweek(col("pickup_datetime"))-2)
    data = data.withColumn("week_of_year", weekofyear(col("pickup_datetime")))
    data = data.withColumn("month_of_year", month(col("pickup_datetime")))
    data = data.withColumn("quarter_of_year", quarter(col("pickup_datetime")))
    data = data.withColumn("year", year(col("pickup_datetime")))
    
    @udf("int")
    def night(hour, weekday):
        if ((hour <= 20) and (hour >= 16) and (weekday < 5)):
            return int(1)
        else:
            return int(0)

    @udf("int")
    def late_night(hour):
        if ((hour <= 6) and (hour >= 20)):
            return int(1)
        else:
            return int(0)
    data = data.withColumn("night", night("hour_of_day", "day_of_week"))
    data = data.withColumn("late_night", late_night("hour_of_day"))
    return data

def add_distance_features(data):

    @udf("float")
    def manhattan(lat1, lon1, lat2, lon2):
        return float(np.abs(lat2 - lat1) + np.abs(lon2 - lon1))
    
    # Location of NYC downtown
    ny = (-74.0063889, 40.7141667)
    # Location of the three airport in NYC
    jfk = (-73.7822222222, 40.6441666667)
    ewr = (-74.175, 40.69)
    lgr = (-73.87, 40.77)
    
    # Features about the distance between pickup/dropoff and airport
    data = data.withColumn("abs_diff_longitude", abs(col("dropoff_longitude")-col("pickup_longitude"))) \
            .withColumn("abs_diff_latitude", abs(col("dropoff_latitude") - col("pickup_latitude")))
    data = data.withColumn("manhattan", col("abs_diff_latitude")+col("abs_diff_longitude"))
    data = data.withColumn("pickup_distance_jfk", manhattan("pickup_longitude", "pickup_latitude", lit(jfk[0]), lit(jfk[1])))
    data = data.withColumn("dropoff_distance_jfk", manhattan("dropoff_longitude", "dropoff_latitude", lit(jfk[0]), lit(jfk[1])))
    data = data.withColumn("pickup_distance_ewr", manhattan("pickup_longitude", "pickup_latitude", lit(ewr[0]), lit(ewr[1])))
    data = data.withColumn("dropoff_distance_ewr", manhattan("dropoff_longitude", "dropoff_latitude", lit(ewr[0]), lit(ewr[1])))
    data = data.withColumn("pickup_distance_lgr", manhattan("pickup_longitude", "pickup_latitude", lit(lgr[0]), lit(lgr[1])))
    data = data.withColumn("dropoff_distance_lgr", manhattan("dropoff_longitude", "dropoff_latitude", lit(lgr[0]), lit(lgr[1])))
    data = data.withColumn("pickup_distance_downtown", manhattan("pickup_longitude", "pickup_latitude", lit(ny[0]), lit(ny[1])))
    data = data.withColumn("dropoff_distance_downtown", manhattan("dropoff_longitude", "dropoff_latitude", lit(ny[0]), lit(ny[1])))
    
    return data

def drop_col(data):
    
    data = data.drop("pickup_datetime") \
            .drop("pickup_longitude") \
            .drop("pickup_latitude") \
            .drop("dropoff_longitude") \
            .drop("dropoff_latitude") \
            .drop("passenger_count") \
            .drop("key")
    
    return data

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

def process_data(worker):
    app_name = "NYC Taxi Fare Prediction with RayDP"
    num_executors = 4
    cores_per_executor = 1
    memory_per_executor = "2GB"
    # Use RayDP to perform data processing
    spark = raydp.init_spark(app_name, num_executors, cores_per_executor, memory_per_executor)
    path_to_csv = '/home/lzhi/Downloads/nyctaxi/nyc_train.csv'
    data = spark.read.format("csv").option("header", "true") \
            .option("inferSchema", "true") \
            .load(path_to_csv)
    # Set spark timezone for processing datetime
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    train_data = clean_up(data)
    train_data = add_time_features(train_data)
    train_data = add_distance_features(train_data)
    train_data = drop_col(train_data)
    ds = create_ml_dataset_from_spark(train_data, 2, args.batch_size)
    features = [field.name for field in list(train_data.schema) if field.name != "fare_amount"]
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
    ray.init(address='auto')
    # Start horovod workers on Ray
    from horovod.ray import RayExecutor
    settings = RayExecutor.create_settings(500)
    executor = RayExecutor(settings, num_hosts=1, num_slots=2, cpus_per_slot=2)
    executor.start()
    torch_ds, num_features = executor.execute_single(process_data)
    # torch_ds = process_data()
    executor.run(train_fn, args=[torch_ds, num_features])
    raydp.stop_spark()
    ray.shutdown()