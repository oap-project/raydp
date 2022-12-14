import ray
from tensorflow import keras
from tensorflow.keras.callbacks import Callback

import raydp
from raydp.tf import TFEstimator
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
data = data.cache()
# Split data into train_dataset and test_dataset
train_df, test_df = random_split(data, [0.9, 0.1], 0)
features = [field.name for field in list(train_df.schema) if field.name != "fare_amount"]

# Define the keras model
model = keras.Sequential(
    [
        keras.layers.InputLayer(input_shape=(len(features),)),
        keras.layers.Dense(256, activation="relu"),
        keras.layers.BatchNormalization(),
        keras.layers.Dense(128, activation="relu"),
        keras.layers.BatchNormalization(),
        keras.layers.Dense(64, activation="relu"),
        keras.layers.BatchNormalization(),
        keras.layers.Dense(32, activation="relu"),
        keras.layers.BatchNormalization(),
        keras.layers.Dense(16, activation="relu"),
        keras.layers.BatchNormalization(),
        keras.layers.Dense(1),
    ]
)

class PrintingCallback(Callback):
    def handle_result(self, results: List[Dict], **info):
        print(results)

# Define the optimizer and loss function
# Then create the tensorflow estimator provided by Raydp
adam = keras.optimizers.Adam(learning_rate=0.001)
loss = keras.losses.MeanSquaredError()
estimator = TFEstimator(num_workers=2, model=model, optimizer=adam, loss=loss,
                        metrics=["mae"], feature_columns=features, label_columns="fare_amount",
                        batch_size=256, num_epochs=10, callbacks=[PrintingCallback()])

# Train the model
estimator.fit_on_spark(train_df, test_df)
# Get the model
model = estimator.get_model()
# shudown raydp and ray
raydp.stop_spark()
ray.shutdown()
