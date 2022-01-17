import ray
from ray.util.sgd.torch import TrainingOperator
from ray.util.sgd import TorchTrainer
from ray import tune
import torch
import torch.nn as nn
import torch.nn.functional as F
from torch.utils.data.dataloader import DataLoader

import raydp
from raydp.torch import TorchEstimator
from raydp.utils import random_split
from raydp.spark import RayMLDataset
from data_process import nyc_taxi_preprocess, NYC_TRAIN_CSV
import ray.data
from ray import train
from ray.train import Trainer, TrainingCallback
import datetime
from typing import List, Dict

# Firstly, You need to init or connect to a ray cluster. Note that you should set include_java to True.
# For more config info in ray, please refer the ray doc. https://docs.ray.io/en/latest/package-ref.html
# ray.init(address="auto")
ray.init()

# After initialize ray cluster, you can use the raydp api to get a spark session
app_name = "NYC Taxi Fare Prediction with RayDP"
num_executors = 1
cores_per_executor = 1
memory_per_executor = "500M"
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
train_df, test_df = random_split(data, [0.9, 0.1], 0)
features = [field.name for field in list(train_df.schema) if field.name != "fare_amount"]

# Convert spark dataframe into ray Dataset
train_dataset = ray.data.from_spark(train_df, parallelism = num_executors)
test_dataset = ray.data.from_spark(test_df, parallelism = num_executors)
# Then convert to torch datasets
feature_dtype = [torch.float] * len(features)
train_dataset = train_dataset.to_torch(feature_columns=features, label_column="fare_amount",
                                               label_column_dtype=torch.float, feature_column_dtypes=feature_dtype, batch_size=64)
test_dataset = test_dataset.to_torch(feature_columns=features, label_column="fare_amount",
                                             label_column_dtype=torch.float, feature_column_dtypes=feature_dtype, batch_size=64)
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
        x = x[0]
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
        optimizer = torch.optim.Adam(nyc_model.parameters(), lr=config['lr'])
        # A quick work-around for https://github.com/ray-project/ray/issues/14352
        self.model, self.optimizer, self.criterion = self.register(
            models=[nyc_model], optimizers=[optimizer], criterion=criterion)
        self.model = self.model[0]
        self.optimizer = self.optimizer[0]
        self.register_data(train_loader=train_dataset, validation_loader=test_dataset)

class PrintingCallback(TrainingCallback):
    def handle_result(self, results: List[Dict], **info):
        print(results)

def train_func(config):
    device = torch.device(f"cuda:{train.local_rank()}" if
                          torch.cuda.is_available() else "cpu")
    if torch.cuda.is_available():
        torch.cuda.set_device(device)

    # Set the args to whatever values you want.
	# If using DistributedSampler, modify it here.
    training_operator = CustomOperator(
        config=config,
        world_rank=train.world_rank(),
        local_rank=train.local_rank(),
        is_distributed=False,
        use_gpu=False,
        device=device)

    training_operator.setup(config)

    for idx in range(config["num_epochs"]):
        train_loader = training_operator._get_train_loader()
        train_stats = training_operator.train_epoch(
            iter(train_loader), epoch_idx=idx)

        validation_loader = training_operator._get_validation_loader()
        val_stats = training_operator.validate(
            val_iterator=iter(validation_loader))
		# Access any values you need
        train.report(epoch=idx)
        train.report(train_info=train_stats)
        train.report(val_info=val_stats)
        train.report(val_loss=val_stats['val_loss'])

    if train.world_rank() == 0:
        return training_operator._get_original_models()
    else:
        return None


trainer = Trainer(backend="torch", num_workers=num_executors, use_gpu=False)
trainer.start()
results = trainer.run(
    train_func, config={"num_epochs": 10, "lr": 0.1},
    callbacks=[PrintingCallback()]
)
final_model = results[0]
trainer.shutdown()

# Or you can perform a hyperparameter search using Ray Tune

# trainable = trainer.to_tune_trainable(train_func)
# analysis = tune.run(trainable, config={
#     "num_epochs": 3,
#     "lr": tune.grid_search([0.005, 0.01, 0.05, 0.1])
# })
# print(analysis.get_best_config(metric="val_loss", mode="min"))
raydp.stop_spark()
ray.shutdown()
