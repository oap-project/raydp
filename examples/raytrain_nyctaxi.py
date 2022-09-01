import ray
from ray import tune
import ray.data
from ray import train
from ray.train import Trainer, TrainingCallback, get_dataset_shard
import torch
import torch.nn as nn
import torch.nn.functional as F

import raydp
from raydp.utils import random_split
from data_process import nyc_taxi_preprocess, NYC_TRAIN_CSV
from typing import List, Dict

# Firstly, You need to init or connect to a ray cluster.
# Note that you should set include_java to True.
# For more config info in ray, please refer the ray doc:
# https://docs.ray.io/en/latest/package-ref.html

# ray.init(address="auto")
ray.init(address="local", num_cpus=6)

# After initialize ray cluster, you can use the raydp api to get a spark session
app_name = "NYC Taxi Fare Prediction with RayDP"
num_executors = 2
cores_per_executor = 1
memory_per_executor = "500M"
spark = raydp.init_spark(app_name, num_executors, cores_per_executor, memory_per_executor)

# Then you can code as you are using spark
# The dataset can be downloaded from：
# https://www.kaggle.com/c/new-york-city-taxi-fare-prediction/data
# Here we just use a subset of the training data
data = spark.read.format("csv").option("header", "true") \
        .option("inferSchema", "true") \
        .load(NYC_TRAIN_CSV)
# Set spark timezone for processing datetime
spark.conf.set("spark.sql.session.timeZone", "UTC")
# Transform the dataset
data = nyc_taxi_preprocess(data)
# Convert spark dataframe into ray Dataset
# pylint: disable=unbalanced-tuple-unpacking
dataset = ray.data.from_spark(data)
# Split data into train_dataset and test_dataset
train_dataset, test_dataset = dataset.split_at_indices([dataset.count()*0.9])
features = [field.name for field in list(data.schema) if field.name != "fare_amount"]

# Define a neural network model
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

class PrintingCallback(TrainingCallback):
    def handle_result(self, results: List[Dict], **info):
        print(results)

def train_epoch(dataset, model, criterion, optimizer):
    model.train()
    train_loss, squared_num, data_size, batch_idx = 0, 0, 0, 0
    for batch_idx, (inputs, targets) in enumerate(dataset):
        # Compute prediction error
        inputs = [inputs[:,i].unsqueeze(1) for i in range(inputs.size(1))]
        outputs = model(*inputs)
        loss = criterion(outputs, targets)
        train_loss += loss.item()
        squared_num += ((outputs - targets)**2).sum().item()
        data_size += targets.size(0)
        # Backpropagation
        optimizer.zero_grad()
        loss.backward()
        optimizer.step()

    train_loss /= (batch_idx + 1)
    train_mse = squared_num/data_size
    return train_mse, train_loss

def test_epoch(dataset, model, criterion):
    model.eval()
    test_loss, squared_num, data_size, batch_idx = 0, 0, 0, 0
    with torch.no_grad():
        for batch_idx, (inputs, targets) in enumerate(dataset):
            # Compute prediction error
            inputs = [inputs[:,i].unsqueeze(1) for i in range(inputs.size(1))]
            outputs = model(*inputs)
            test_loss += criterion(outputs, targets).item()
            squared_num += ((outputs - targets)**2).sum().item()
            data_size += targets.size(0)
    test_loss /= (batch_idx + 1)
    test_mse = squared_num/data_size
    return test_mse, test_loss

def train_func(config):
    num_epochs = config["num_epochs"]
    lr = config["lr"]
    batch_size = config["batch_size"]
    # Then convert to torch datasets
    train_data_shard = get_dataset_shard("train")
    train_dataset = train_data_shard.to_torch(feature_columns=features,
                                              label_column="fare_amount",
                                              label_column_dtype=torch.float,
                                              feature_column_dtypes=torch.float,
                                              batch_size=batch_size)
    test_data_shard = get_dataset_shard("test")
    test_dataset = test_data_shard.to_torch(feature_columns=features,
                                            label_column="fare_amount",
                                            label_column_dtype=torch.float,
                                            feature_column_dtypes=torch.float,
                                            batch_size=batch_size)
    model = NYC_Model(len(features))
    model = train.torch.prepare_model(model)
    criterion = nn.SmoothL1Loss()
    optimizer = torch.optim.Adam(model.parameters(), lr=lr)
    loss_results = []
    for epoch in range(num_epochs):
        train_mse, train_loss = train_epoch(train_dataset, model, criterion, optimizer)
        test_mse, test_loss = test_epoch(test_dataset, model, criterion)
        train.report(epoch = epoch, train_mse = train_mse, train_loss = train_loss)
        train.report(epoch = epoch, test_mse = test_mse, test_loss=test_loss)
        loss_results.append(test_loss)

trainer = Trainer(backend="torch", num_workers=num_executors)
trainer.start()
results = trainer.run(
    train_func, config={"num_epochs": 10, "lr": 0.001, "batch_size": 64},
    callbacks=[PrintingCallback()],
    dataset={
        "train": train_dataset,
        "test": test_dataset
    }
)
trainer.shutdown()

# Or you can perform a hyperparameter search using Ray Tune

# trainable = trainer.to_tune_trainable(train_func, dataset={
#         "train": train_dataset,
#         "test": test_dataset
#     })
# analysis = tune.run(trainable, config={
#     "num_epochs": 3,
#     "batch_size": 64,
#     "lr": tune.grid_search([0.005, 0.01, 0.05, 0.1])
# })
# print(analysis.get_best_config(metric="test_mse", mode="min"))
raydp.stop_spark()
ray.shutdown()
