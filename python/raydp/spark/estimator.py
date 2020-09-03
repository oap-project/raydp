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

from abc import ABC, abstractmethod
from typing import Any, NoReturn

from raydp.spark.utils import df_type_check


class EstimatorInterface(ABC):
    """
    A scikit-learn like API.
    """

    @abstractmethod
    def fit(self, df, **kwargs) -> NoReturn:
        """
        Fit the model on the df. The df should be ether spark DataFrame or koalas DataFrame.
        """
        df_type_check(df)

    @abstractmethod
    def evaluate(self, df: Any, **kwargs) -> NoReturn:
        """
        Evaluate on the trained model. The df should be ether spark DataFrame or koalas
        DataFrame. This should be called after call fit.
        """
        df_type_check(df)

    @abstractmethod
    def get_model(self) -> Any:
        """
        Get the trained model
        :return the model
        """
        pass

    @abstractmethod
    def save(self, file_path) -> NoReturn:
        """
        Save the trained model to the given file path
        :param file_path: the file path
        """
        pass

    @abstractmethod
    def restore(self, file_path) -> NoReturn:
        """
        Restore the model
        :param file_path: the model saved file path
        """
        pass

    @abstractmethod
    def shutdown(self) -> NoReturn:
        """
        Shutdown the estimator
        """
        pass
