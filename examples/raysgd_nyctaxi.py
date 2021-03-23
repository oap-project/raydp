import ray
from ray.util.sgd.torch import TrainingOperator
from ray.util.sgd import TorchTrainer
import torch
import torch.nn as nn
import torch.nn.functional as F
from torch.utils.data.dataloader import DataLoader

import raydp
from raydp.torch import TorchEstimator
from raydp.utils import random_split
from raydp.spark import create_ml_dataset_from_spark
from data_process import nyc_taxi_preprocess, NYC_TRAIN_CSV

# Firstly, You need to init or connect to a ray cluster. Note that you should set include_java to True.
# For more config info in ray, please refer the ray doc. https://docs.ray.io/en/latest/package-ref.html
# ray.init(address="auto")
ray.init()

# After initialize ray cluster, you can use the raydp api to get a spark session
app_name = "NYC Taxi Fare Prediction with RayDP"
num_executors = 4
cores_per_executor = 1
memory_per_executor = "2GB"
spark = raydp.init_spark(app_name, num_executors, cores_per_executor, memory_per_executor)

# Then you can code as you are using spark
# The dataset can be downloaded from https://www.kaggle.com/c/new-york-city-taxi-fare-prediction/data
# Here we just use a subset of the training data
data = spark.read.format("csv").option("header", "true") \
        .option("inferSchema", "true") \
        .load(NYC_TRAIN_CSV)
# Set spark timezone for processing datetime
spark.conf.set("spark.sql.session.timeZone", "UTC")
# Transform the dataset
data = nyc_taxi_preprocess(data)
# Split data into train_dataset and test_dataset
train_df, test_df = random_split(data, [0.9, 0.1])
features = [field.name for field in list(train_df.schema) if field.name != "fare_amount"]
# Convert spark dataframe into ML Dataset
train_dataset = create_ml_dataset_from_spark(train_df, num_executors, 32)
test_dataset = create_ml_dataset_from_spark(test_df, num_executors, 32)
# Then convert to torch datasets
train_dataset = train_dataset.to_torch(feature_columns=features, label_column="fare_amount")
test_dataset = test_dataset.to_torch(feature_columns=features, label_column="fare_amount")
# Define a neural network model
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

class CustomOperator(TrainingOperator):
    def setup(self, config):
        nyc_model = NYC_Model(len(features))
        criterion = nn.SmoothL1Loss()
        optimizer = torch.optim.Adam(nyc_model.parameters(), lr=0.001)
        # A quick work-around for https://github.com/ray-project/ray/issues/14352
        self.model, self.optimizer, self.criterion = self.register(
            models=[nyc_model], optimizers=[optimizer], criterion=criterion)
        self.model = self.model[0]
        self.optimizer = self.optimizer[0]
        # Get the corresponging shard
        train_shard = train_dataset.get_shard(self.world_rank)
        train_loader = DataLoader(train_shard, batch_size=32)
        test_shard = test_dataset.get_shard(self.world_rank)
        val_loader = DataLoader(test_shard, batch_size=32)
        self.register_data(train_loader=train_loader, validation_loader=val_loader)

trainer = TorchTrainer(training_operator_cls=CustomOperator,
                       num_workers=num_executors,
                       add_dist_sampler=False,
                       num_cpus_per_worker=2)
for i in range(100):
    stats = trainer.train()
    print(stats)
    val_stats = trainer.validate()
    print(val_stats)

trainer.shutdown()
raydp.stop_spark()
ray.shutdown()