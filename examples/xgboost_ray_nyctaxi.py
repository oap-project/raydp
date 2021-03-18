import ray
import numpy as np
from pyspark.sql.functions import *
# XGBoost on ray is needed to run this example.
# Please refer to https://docs.ray.io/en/latest/xgboost-ray.html to install it.
from xgboost_ray import RayDMatrix, train, RayParams
import raydp
from raydp.utils import random_split
from raydp.spark import create_ml_dataset_from_spark
from data_process import nyc_taxi_preprocess, NYC_TRAIN_CSV

# connect to ray cluster
ray.init(address='auto')
# ray.init()
# After ray.init, you can use the raydp api to get a spark session
app_name = "NYC Taxi Fare Prediction with RayDP"
num_executors = 4
cores_per_executor = 1
memory_per_executor = "2GB"
spark = raydp.init_spark(app_name, num_executors, cores_per_executor, memory_per_executor)
data = spark.read.format("csv").option("header", "true") \
        .option("inferSchema", "true") \
        .load(NYC_TRAIN_CSV)
# Set spark timezone for processing datetime
spark.conf.set("spark.sql.session.timeZone", "UTC")
# Transform the dataset
data = nyc_taxi_preprocess(data)
# Split data into train_dataset and test_dataset
train_df, test_df = random_split(data, [0.9, 0.1])
# Convert spark dataframe into ML Dataset
train_dataset = create_ml_dataset_from_spark(train_df, 2, 32)
test_dataset = create_ml_dataset_from_spark(test_df, 2, 32)
# Then convert them into DMatrix used by xgboost
dtrain = RayDMatrix(train_dataset, label='fare_amount')
dtest = RayDMatrix(test_dataset, label='fare_amount')
# Configure the XGBoost model
config = {
    "tree_method": "hist",
    "eval_metric": ["logloss", "error"],
}
evals_result = {}
# Train the model
bst = train(
        config,
        dtrain,
        evals=[(dtest, "eval")],
        evals_result=evals_result,
        ray_params=RayParams(max_actor_restarts=1, num_actors=2, cpus_per_actor=8),
        num_boost_round=10)
# print evaluation stats
print("Final validation error: {:.4f}".format(
        evals_result["eval"]["error"][-1]))
raydp.stop_spark()
ray.shutdown()