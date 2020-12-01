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
from typing import Any, NoReturn, Optional

from ray.util.data import MLDataset


class EstimatorInterface(ABC):
    """
    A scikit-learn like API.
    """

    @abstractmethod
    def fit(self,
            train_ds: MLDataset,
            evaluate_ds: Optional[MLDataset] = None) -> NoReturn:
        """Train or evaluate the model.

        :param train_ds: the model will train on the MLDataset
        :param evaluate_ds: if this is provided, the model will evaluate on the MLDataset
        """

    @abstractmethod
    def get_model(self) -> Any:
        """Get the trained model

        :return the model
        """

    @abstractmethod
    def save(self, file_path) -> NoReturn:
        """Save the trained model to the given file path

        :param file_path: the file path
        """

    @abstractmethod
    def restore(self, file_path) -> NoReturn:
        """Restore the model

        :param file_path: the model saved file path
        """

    @abstractmethod
    def shutdown(self) -> NoReturn:
        """Shutdown the estimator"""
