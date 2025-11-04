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

import os
import sys
import shutil
import platform
import pytest
import pyspark
import numpy as np
from pyspark.sql.functions import rand

from raydp.xgboost import XGBoostEstimator
from raydp.utils import random_split

@pytest.mark.parametrize("use_fs_directory", [True, False])
def test_xgb_estimator(spark_on_ray_small, use_fs_directory):
    if platform.system() == "Darwin":
        pytest.skip("Skip xgboost test on MacOS")
    spark = spark_on_ray_small

    # calculate z = 3 * x + 4 * y + 5
    df: pyspark.sql.DataFrame = spark.range(0, 100000)
    df = df.withColumn("x", rand() * 100)  # add x column
    df = df.withColumn("y", rand() * 1000)  # ad y column
    df = df.withColumn("z", df.x * 3 + df.y * 4 + rand() + 5)  # ad z column
    df = df.select(df.x, df.y, df.z)

    train_df, test_df = random_split(df, [0.7, 0.3])
    params = {}
    estimator = XGBoostEstimator(params, "z", resources_per_worker={"CPU": 1})
    if use_fs_directory:
        dir = os.path.dirname(os.path.realpath(__file__)) + "/test_xgboost"
        uri = "file://" + dir
        estimator.fit_on_spark(train_df, test_df, fs_directory=uri)
    else:
        estimator.fit_on_spark(train_df, test_df)
    print(estimator.get_model().inplace_predict(np.asarray([[1,2]])))
    if use_fs_directory:
        shutil.rmtree(dir)

if __name__ == '__main__':
    sys.exit(pytest.main(["-v", __file__]))