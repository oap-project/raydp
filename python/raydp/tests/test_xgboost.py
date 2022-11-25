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

import pytest
import databricks.koalas as ks

from raydp.xgboost import XGBoostEstimator
from raydp.utils import random_split

@pytest.mark.parametrize("use_fs_directory", [True, False])
def test_xgb_estimator(spark_on_ray_small, use_fs_directory):
    spark = spark_on_ray_small

    # calculate z = 3 * x + 4 * y + 5
    df: ks.DataFrame = ks.range(0, 100000)
    df["x"] = df["id"] + 100
    df["y"] = df["id"] + 1000
    df["z"] = df["x"] * 3 + df["y"] * 4 + 5
    df = df.astype("float")

    train_df, test_df = random_split(df, [0.7, 0.3])
    params = {}
    estimator = XGBoostEstimator(params, "z", resources_per_worker={"CPU": 1})
    estimator.fit_on_spark(train_df=train_df, evaluate_df=test_df)

if __name__ == '__main__':
    import ray, raydp
    ray.init()
    spark = raydp.init_spark('a', 1, 1, '500m')
    test_xgb_estimator(spark, False)