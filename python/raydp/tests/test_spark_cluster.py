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

import pytest

import raydp.spark.context as context


def test_spark_standalone(ray_cluster):
    spark = context.init_spark("test", 1, 1, "500 M", resource_manager="standalone")
    result = spark.range(0, 10).count()
    assert result == 10
    context.stop_spark()


def test_spark_native(ray_cluster):
    spark = context.init_spark("test", 1, 1, "500 M", resource_manager="ray")
    result = spark.range(0, 10).count()
    assert result == 10
    context.stop_spark()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
