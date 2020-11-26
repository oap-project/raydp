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

from typing import NoReturn
from typing import Optional, Union

from raydp.utils import convert_to_spark

DF = Union["pyspark.sql.DataFrame", "koalas.DataFrame"]
OPTIONAL_DF = Union[Optional["pyspark.sql.DataFrame"], Optional["koalas.DataFrame"]]


class SparkEstimatorInterface:
    def _check_and_convert(self, df):
        train_df, _ = convert_to_spark(df)
        return train_df

    def fit_on_spark(self,
                     train_df: DF,
                     evaluate_df: OPTIONAL_DF = None) -> NoReturn:
        """Fit and evaluate the model on the Spark or koalas DataFrame.

        :param train_df the DataFrame which the model will train on.
        :param evaluate_df the optional DataFrame which the model evaluate on it
        """
