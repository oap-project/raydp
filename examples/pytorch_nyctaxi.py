import ray
import torch
import torch.nn as nn
import torch.nn.functional as F

import raydp
from raydp.torch import TorchEstimator
from raydp.utils import random_split

from data_process import nyc_taxi_preprocess, NYC_TRAIN_CSV
from typing import List, Dict

# Firstly, You need to init or connect to a ray cluster.
# Note that you should set include_java to True.
# For more config info in ray, please refer the ray doc:
# https://docs.ray.io/en/latest/package-ref.html
# ray.init(address="auto")
ray.init(address="local", num_cpus=4)

# After initialize ray cluster, you can use the raydp api to get a spark session
app_name = "NYC Taxi Fare Prediction with RayDP"
num_executors = 1
cores_per_executor = 1
memory_per_executor = "500M"
spark = raydp.init_spark(app_name, num_executors, cores_per_executor, memory_per_executor)

# Then you can code as you are using spark
# The dataset can be downloaded from:
# https://www.kaggle.com/c/new-york-city-taxi-fare-prediction/data
# Here we just use a subset of the training data
data = spark.read.format("csv").option("header", "true") \
        .option("inferSchema", "true") \
        .load(NYC_TRAIN_CSV)
# Set spark timezone for processing datetime
spark.conf.set("spark.sql.session.timeZone", "UTC")
# Transform the dataset
data = nyc_taxi_preprocess(data)
# Split data into train_dataset and test_dataset
train_df, test_df = random_split(data, [0.9, 0.1], 0)
features = [field.name for field in list(train_df.schema) if field.name != "fare_amount"]
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

    def forward(self, x):
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

nyc_model = NYC_Model(len(features))
criterion = nn.SmoothL1Loss()
optimizer = torch.optim.Adam(nyc_model.parameters(), lr=0.001)
# Create a distributed estimator based on the raydp api
estimator = TorchEstimator(num_workers=1, model=nyc_model, optimizer=optimizer, loss=criterion,
                           feature_columns=features, feature_types=torch.float,
                           label_column="fare_amount", label_type=torch.float,
                           batch_size=64, num_epochs=30,
                           metrics_name = ["MeanAbsoluteError", "MeanSquaredError"],
                           use_ccl=False)
# Train the model
estimator.fit_on_spark(train_df, test_df)
# Get the trained model
model = estimator.get_model()
# shutdown raydp and ray
raydp.stop_spark()
ray.shutdown()
