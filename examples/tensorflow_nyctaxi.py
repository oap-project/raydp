import ray
from tensorflow import keras

import raydp
from raydp.tf import TFEstimator
from raydp.utils import random_split

from data_process import nyc_taxi_preprocess, NYC_TRAIN_CSV

# Firstly, You need to init or connect to a ray cluster. Note that you should set include_java to True.
# For more config info in ray, please refer the ray doc. https://docs.ray.io/en/latest/package-ref.html
ray.init(address="auto")
# ray.init()

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
train_df, test_df = random_split(data, [0.9, 0.1])
features = [field.name for field in list(train_df.schema) if field.name != "fare_amount"]

# Define the keras model
# Each feature will be regarded as an input with shape (1,）
inTensor = []
for _ in range(len(features)):
    inTensor.append(keras.Input((1,)))
concatenated = keras.layers.concatenate(inTensor)
fc1 = keras.layers.Dense(256, activation='relu')(concatenated)
bn1 = keras.layers.BatchNormalization()(fc1)
fc2 = keras.layers.Dense(128, activation='relu')(bn1)
bn2 = keras.layers.BatchNormalization()(fc2)
fc3 = keras.layers.Dense(64, activation='relu')(bn2)
bn3 = keras.layers.BatchNormalization()(fc3)
fc4 = keras.layers.Dense(32, activation='relu')(bn3)
bn4 = keras.layers.BatchNormalization()(fc4)
fc5 = keras.layers.Dense(16, activation='relu')(bn4)
bn5 = keras.layers.BatchNormalization()(fc5)
fc6 = keras.layers.Dense(1)(bn5)
model = keras.models.Model(inTensor, fc6)

# Define the optimizer and loss function
# Then create the tensorflow estimator provided by Raydp
adam = keras.optimizers.Adam(lr=0.001)
loss = keras.losses.MeanSquaredError()
estimator = TFEstimator(num_workers=1, model=model, optimizer=adam, loss=loss, metrics=["mae"],
                        feature_columns=features, label_column="fare_amount", batch_size=256, num_epochs=30,
                        config={"fit_config": {"steps_per_epoch": train_df.count() // 256}})

# Train the model
estimator.fit_on_spark(train_df, test_df)

# shudown raydp and ray
estimator.shutdown()
raydp.stop_spark()
ray.shutdown()
