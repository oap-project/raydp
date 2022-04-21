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
import sys
import torch
import raydp
from raydp.torch import TorchEstimator

def test_torch_estimator(spark_on_ray_small):
    ##prepare the data
    customers = [
        (1,'James', 21, 6),
        (2, "Liz", 25, 8),
        (3, "John", 31, 6),
        (4, "Jennifer", 45, 7),
        (5, "Robert", 41, 5),
        (6, "Sandra", 45, 8)
    ]
    df = spark_on_ray_small.createDataFrame(customers, ["cID", "name", "age", "grade"])

    ##create model
    model = torch.nn.Sequential(torch.nn.Linear(1, 2), torch.nn.Linear(2,1))
    optimizer = torch.optim.Adam(model.parameters())
    loss = torch.nn.MSELoss()

    #config
    estimator = TorchEstimator(
        model = model,
        optimizer = optimizer,
        loss = loss,
        num_workers = 3,
        num_epochs = 5,
        feature_columns = ["age"],
        feature_types = torch.float,
        label_column = "grade",
        label_type = torch.float,
        batch_size = 2
    )
    estimator.fit_on_spark(df)

    estimator.shutdown()

if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))