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

import glob
from typing import Any, Callable, Dict, List, NoReturn, Optional

import pandas as pd
import pyarrow as pa
import pyarrow.plasma as plasma
import pyspark
import ray
from pyarrow.plasma import PlasmaClient
from pyspark.sql.session import SparkSession
import numpy as np

from raydp.spark import SparkCluster
from raydp.spark.ray_cluster_master import RayClusterMaster, RAYDP_CP
from raydp.parallel import RayDataset

from collections import Iterator


class RayCluster(SparkCluster):
    def __init__(self):
        super().__init__(None)
        self._app_master_bridge = None
        self._set_up_master(None, None)
        self._spark_session: SparkSession = None

    def _set_up_master(self, resources: Dict[str, float], kwargs: Dict[Any, Any]):
        # TODO: specify the app master resource
        self._app_master_bridge = RayClusterMaster()
        self._app_master_bridge.start_up()

    def _set_up_worker(self, resources: Dict[str, float], kwargs: Dict[str, str]):
        raise Exception("Unsupported operation")

    def get_cluster_url(self) -> str:
        return self._app_master_bridge.get_master_url()

    def get_spark_session(self,
                          app_name: str,
                          num_executors: int,
                          executor_cores: int,
                          executor_memory: int,
                          extra_conf: Dict[str, str] = None) -> SparkSession:
        if self._spark_session is not None:
            return self._spark_session

        if extra_conf is None:
            extra_conf = {}
        extra_conf["spark.executor.instances"] = str(num_executors)
        extra_conf["spark.executor.cores"] = str(executor_cores)
        extra_conf["spark.executor.memory"] = str(executor_memory)
        extra_conf["spark.jars"] = ",".join(glob.glob(RAYDP_CP))
        spark_builder = SparkSession.builder
        for k, v in extra_conf.items():
            spark_builder.config(k, v)
        self._spark_session =\
            spark_builder.appName(app_name).master(self.get_cluster_url()).getOrCreate()
        return self._spark_session

    def save_to_ray(self, df: pyspark.sql.DataFrame, num_shards: int) -> RayDataset:
        # call java function from python
        sql_context = df.sql_ctx
        jvm = sql_context.sparkSession.sparkContext._jvm
        jdf = df._jdf
        object_store_writer = jvm.org.apache.spark.sql.raydp.ObjectStoreWriter(jdf)
        records = object_store_writer.save()

        worker = ray.worker.global_worker

        batch_list: List[RecordBatch] = []
        for record in records:
            owner_address = record.ownerAddress()
            object_id = ray.ObjectID(record.objectId())
            num_records = record.numRecords()
            # Register the ownership of the ObjectRef
            worker.core_worker.deserialize_and_register_object_ref(
                object_id.binary(), ray.ObjectRef.nil(), owner_address)

            batch_list.append(RecordBatch([object_id], [num_records]))
        batch_set = RecordBatchSet(batch_list)

        return RayClusterSharedDataset(object_ids, sizes)

    def stop(self):
        if self._spark_session is not None:
            self._spark_session.stop()
            self._spark_session = None

        if self._app_master_bridge is not None:
            self._app_master_bridge.stop()
            self._app_master_bridge = None


class RecordBatch:
    def __init__(self,
                 object_ids: List[ray.ObjectRef],
                 sizes: List[int]):
        self._object_ids: List[ray.ObjectRef] = object_ids
        self._sizes: List[int] = sizes
        self._resolved: bool = False

    def batch_size(self) -> int:
        return sum(self._sizes)

    def _fetch_objects_without_deserialization(self, object_ids, timeout=None) -> NoReturn:
        """
        This is just fetch object from remote object store to local and without deserialization.
        :param object_ids: Object ID of the object to get or a list of object IDs to
            get.
        :param timeout (Optional[float]): The maximum amount of time in seconds to
            wait before returning.
        """
        is_individual_id = isinstance(object_ids, ray.ObjectID)
        if is_individual_id:
            object_ids = [object_ids]

        if not isinstance(object_ids, list):
            raise ValueError("'object_ids' must either be an object ID "
                             "or a list of object IDs.")

        worker = ray.worker.global_worker
        worker.check_connected()
        timeout_ms = int(timeout * 1000) if timeout else -1
        worker.core_worker.get_objects(object_ids, worker.current_task_id, timeout_ms)

    def resolve(self):
        self._fetch_objects_without_deserialization(self._object_ids)
        self._resolved = True

    def __iter__(self) -> Iterator[pd.DataFrame]:
        index = 0
        length = len(self._sizes)
        while index < length:
            object_id = self._object_ids[index]
            assert object_id is not None
            data = self._get_data_func(object_id)
            reader = pa.ipc.open_stream(data)
            tb = reader.read_all()
            df = tb.to_pandas()
            yield df
            index += 1


class RecordBatchSet:
    def __init__(self, record_batches: List[RecordBatch]):
        self._record_batches = record_batches

    def total_size(self) -> int:
        return sum([batch.batch_size() for batch in self._record_batches])

    def partition_sizes(self) -> List[int]:
        return [batch.batch_size() for batch in self._record_batches]
