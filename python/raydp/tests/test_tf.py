#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import sys

import pyspark
import pytest
import tensorflow.keras as keras
from pyspark.sql.functions import rand

from raydp.spark import context
from raydp.spark.tf.estimator import TFEstimator
from raydp.spark.utils import random_split


def test_tf_estimator(ray_cluster):
    spark = context.init_spark("test", 2, 1, "500M")

    # ---------------- data process with Spark ------------
    # calculate z = 3 * x + 4 * y + 5
    df: pyspark.sql.DataFrame = spark.range(0, 100000)
    df = df.withColumn("x", rand() * 100)  # add x column
    df = df.withColumn("y", rand() * 1000)  # ad y column
    df = df.withColumn("z", df.x * 3 + df.y * 4 + rand() + 5)  # ad z column
    df = df.select(df.x, df.y, df.z)

    train_df, test_df = random_split(df, [0.7, 0.3])

    # create model
    input_1 = keras.Input(shape=(1,))
    input_2 = keras.Input(shape=(1,))

    concatenated = keras.layers.concatenate([input_1, input_2])
    output = keras.layers.Dense(1, activation='sigmoid')(concatenated)
    model = keras.Model(inputs=[input_1, input_2],
                        outputs=output)

    optimizer = keras.optimizers.Adam(0.01)
    loss = keras.losses.MeanSquaredError()

    estimator = TFEstimator(num_workers=2,
                            model=model,
                            optimizer=optimizer,
                            loss=loss,
                            metrics=["accuracy", "mse"],
                            feature_columns=["x", "y"],
                            label_column="z",
                            batch_size=1000,
                            num_epochs=2,
                            config={"fit_config": {"steps_per_epoch": 2}})

    estimator.fit(train_df)
    estimator.evaluate(test_df)

    estimator.shutdown()
    context.stop_spark()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
