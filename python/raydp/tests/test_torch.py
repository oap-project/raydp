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
import os
import sys
import shutil
import torch

# https://spark.apache.org/docs/latest/api/python/migration_guide/koalas_to_pyspark.html
# import databricks.koalas as ks
import pyspark.pandas as ps

from raydp.torch import TorchEstimator
from raydp.utils import random_split

@pytest.mark.parametrize("use_fs_directory", [True, False])
def test_torch_estimator(spark_on_ray_small, use_fs_directory):
    # ---------------- data process with koalas ------------
    spark = spark_on_ray_small

    # calculate z = 3 * x + 4 * y + 5
    df: ps.DataFrame = ps.range(0, 100000)
    df["x"] = df["id"] + 100
    df["y"] = df["id"] + 1000
    df["z"] = df["x"] * 3 + df["y"] * 4 + 5
    df = df.astype("float")

    train_df, test_df = random_split(df, [0.7, 0.3])

    # ---------------- ray sgd -------------------------
    # create the model
    class LinearModel(torch.nn.Module):
        def __init__(self):
            super(LinearModel, self).__init__()
            self.linear = torch.nn.Linear(2, 1)

        def forward(self, x):
            return self.linear(x)

    model = LinearModel()
    # create the optimizer
    optimizer = torch.optim.Adam(model.parameters())
    # create the loss
    loss = torch.nn.MSELoss()
    # create lr_scheduler

    def lr_scheduler_creator(optimizer, config):
        return torch.optim.lr_scheduler.MultiStepLR(
            optimizer, milestones=[150, 250, 350], gamma=0.1)

    # create the estimator
    estimator = TorchEstimator(num_workers=2,
                               model=model,
                               optimizer=optimizer,
                               loss=loss,
                               lr_scheduler_creator=lr_scheduler_creator,
                               feature_columns=["x", "y"],
                               feature_types=torch.float,
                               label_column="z",
                               label_type=torch.float,
                               batch_size=1000,
                               num_epochs=2,
                               use_gpu=False)

    # train the model
    if use_fs_directory:
        dir = os.path.dirname(__file__) + "/test_torch"
        uri = "file://" + dir
        estimator.fit_on_spark(train_df, test_df, fs_directory=uri)
    else:
        estimator.fit_on_spark(train_df, test_df)
    model = estimator.get_model()
    result = model(torch.Tensor([[0, 0], [1, 1]]))
    assert result.shape == (2, 1)
    if use_fs_directory:
        shutil.rmtree(dir)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
