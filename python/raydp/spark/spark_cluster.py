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

from abc import abstractmethod
from typing import Dict

import pyspark

from raydp.parallel import PandasDataset
from raydp.services import Cluster


class SparkCluster(Cluster):
    """
    A abstract class to support save Spark DataFrame to ray object store.
    """

    @abstractmethod
    def get_spark_session(self,
                          app_name: str,
                          num_executors: int,
                          executor_cores: int,
                          executor_memory: int,
                          extra_conf: Dict[str, str] = None) -> pyspark.sql.SparkSession:
        pass

    @abstractmethod
    def save_to_ray(self,
                    df: pyspark.sql.DataFrame,
                    num_shards: int) -> PandasDataset:
        pass
