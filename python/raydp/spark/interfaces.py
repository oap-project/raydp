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
from typing import Union

from raydp.utils import df_type_check


class SparkEstimatorInterface:
    def fit_on_spark(self,
                     df: Union["pyspark.sql.DataFrame", "koalas.DataFrame"],
                     **kwargs) -> NoReturn:
        """
        Fit the model on the df. The df should be ether spark DataFrame or koalas DataFrame.
        """
        df_type_check(df)

    def evaluate_on_spark(self,
                          df: Union["pyspark.sql.DataFrame", "koalas.DataFrame"],
                          **kwargs) -> NoReturn:
        """
        Evaluate on the trained model. The df should be ether spark DataFrame or koalas
        DataFrame. This should be called after call fit.
        """
        df_type_check(df)
