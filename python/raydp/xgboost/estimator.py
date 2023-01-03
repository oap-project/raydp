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

from typing import Any, Callable, List, NoReturn, Optional, Union, Dict

from raydp.estimator import EstimatorInterface
from raydp.spark.interfaces import SparkEstimatorInterface, DF, OPTIONAL_DF
from raydp import stop_spark
from raydp.spark import spark_dataframe_to_ray_dataset

import ray
from ray.air.config import ScalingConfig, RunConfig, FailureConfig
from ray.data.dataset import Dataset
from ray.train.xgboost import XGBoostTrainer, XGBoostCheckpoint

class XGBoostEstimator(EstimatorInterface, SparkEstimatorInterface):
    def __init__(self,
                 xgboost_params: Dict,
                 label_column: str,
                 dmatrix_params: Dict = None,
                 num_workers: int = 1,
                 resources_per_worker: Optional[Dict[str, float]] = None,
                 shuffle: bool = True):
        """
        :param xgboost_params: XGBoost training parameters.
              Refer to `XGBoost documentation <https://xgboost.readthedocs.io/>`_
              for a list of possible parameters.
        :param label_column: Name of the label column. A column with this name
              must be present in the training dataset passed to fit() later.
        :param dmatrix_params: Dict of ``dataset name:dict of kwargs`` passed to respective
              :class:`xgboost_ray.RayDMatrix` initializations, which in turn are passed
              to ``xgboost.DMatrix`` objects created on each worker. For example, this can
              be used to add sample weights with the ``weights`` parameter.
        :param num_workers: the number of workers to do the distributed training.
        :param resources_per_worker: the resources defined in this Dict will be reserved for
              each worker. The ``CPU`` and ``GPU`` keys (case-sensitive) can be defined to
              override the number of CPU/GPUs used by each worker.
        :param shuffle: whether to shuffle the data
        """
        self._xgboost_params = xgboost_params
        self._label_column = label_column
        self._dmatrix_params = dmatrix_params
        self._num_workers = num_workers
        self._resources_per_worker = resources_per_worker
        self._shuffle = shuffle

    def fit(self,
            train_ds: Dataset,
            evaluate_ds: Optional[Dataset] = None,
            max_retries=3) -> NoReturn:
        scaling_config = ScalingConfig(num_workers=self._num_workers,
                                      resources_per_worker=self._resources_per_worker)
        run_config = RunConfig(failure_config=FailureConfig(max_failures=max_retries))
        if self._shuffle:
            train_ds = train_ds.random_shuffle()
            if evaluate_ds:
                evaluate_ds = evaluate_ds.random_shuffle()
        datasets = {"train": train_ds}
        if evaluate_ds:
            datasets["evaluate"] = evaluate_ds
        trainer = XGBoostTrainer(scaling_config=scaling_config,
                                datasets=datasets,
                                label_column=self._label_column,
                                params=self._xgboost_params,
                                dmatrix_params=self._dmatrix_params,
                                run_config=run_config)
        self._results = trainer.fit()

    def fit_on_spark(self,
                     train_df: DF,
                     evaluate_df: OPTIONAL_DF = None,
                     max_retries=3,
                     fs_directory: Optional[str] = None,
                     compression: Optional[str] = None,
                     stop_spark_after_conversion=False):
        train_df = self._check_and_convert(train_df)
        evaluate_ds = None
        if fs_directory is not None:
            app_id = train_df.sql_ctx.sparkSession.sparkContext.applicationId
            path = fs_directory.rstrip("/") + f"/{app_id}"
            train_df.write.parquet(path+"/train", compression=compression)
            train_ds = ray.data.read_parquet(path+"/train")
            if evaluate_df is not None:
                evaluate_df = self._check_and_convert(evaluate_df)
                evaluate_df.write.parquet(path+"/test", compression=compression)
                evaluate_ds = ray.data.read_parquet(path+"/test")
        else:
            train_ds = spark_dataframe_to_ray_dataset(train_df,
                                                  parallelism=self._num_workers,
                                                  _use_owner=stop_spark_after_conversion)
            if evaluate_df is not None:
                evaluate_df = self._check_and_convert(evaluate_df)
                evaluate_ds = spark_dataframe_to_ray_dataset(evaluate_df,
                                                         parallelism=self._num_workers,
                                                         _use_owner=stop_spark_after_conversion)
        if stop_spark_after_conversion:
            stop_spark(cleanup_data=False)
        return self.fit(
            train_ds, evaluate_ds, max_retries)

    def get_model(self):
        return XGBoostCheckpoint.from_checkpoint(self._results.checkpoint).get_model()
