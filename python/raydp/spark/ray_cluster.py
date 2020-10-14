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
from collections import Iterator
from typing import Any, Dict, Generic, Iterable, List, NoReturn

import pandas as pd
import pyarrow as pa
import pyspark
import ray
from pyspark.sql.session import SparkSession

import raydp.parallel.general_dataset as parallel_dataset
from raydp.parallel import PandasDataset
from raydp.utils import divide_blocks
from .ray_cluster_master import RayClusterMaster, RAYDP_CP
from .spark_cluster import SparkCluster


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

    def save_to_ray(self, df: pyspark.sql.DataFrame, num_shards: int) -> PandasDataset:
        # call java function from python
        df = df.repartition(num_shards)
        sql_context = df.sql_ctx
        jvm = sql_context.sparkSession.sparkContext._jvm
        jdf = df._jdf
        object_store_writer = jvm.org.apache.spark.sql.raydp.ObjectStoreWriter(jdf)
        records = object_store_writer.save()

        worker = ray.worker.global_worker

        blocks: List[ray.ObjectRef] = []
        block_sizes: List[int] = []
        for record in records:
            owner_address = record.ownerAddress()
            object_id = ray.ObjectID(record.objectId())
            num_records = record.numRecords()
            # Register the ownership of the ObjectRef
            worker.core_worker.deserialize_and_register_object_ref(
                object_id.binary(), ray.ObjectRef.nil(), owner_address)

            blocks.append(object_id)
            block_sizes.append(num_records)

        divided_blocks = divide_blocks(block_sizes, num_shards)
        record_batch_set: List[RecordBatch] = []
        for i in range(num_shards):
            indexes = divided_blocks[i]
            object_ids = [blocks[index] for index in indexes]
            record_batch_set.append(RecordBatch(object_ids))

        # TODO: we should specify the resource spec for each shard
        ds = parallel_dataset.from_iterators(generators=record_batch_set,
                                             name="spark_df")

        def resolve_fn(it: "Iterable[RecordBatch]") -> "Iterator[RecordBatch]":
            for item in it:
                item.resolve()
                yield item
        return ds.transform(resolve_fn, ".RecordBatch#resolve()").flatten().to_pandas(None)

    def stop(self):
        if self._spark_session is not None:
            self._spark_session.stop()
            self._spark_session = None

        if self._app_master_bridge is not None:
            self._app_master_bridge.stop()
            self._app_master_bridge = None


class RecordBatch:
    def __init__(self, object_ids: List[ray.ObjectRef]):
        self._object_ids: List[ray.ObjectRef] = object_ids
        self._resolved: bool = False

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
        if self._resolved:
            return
        self._fetch_objects_without_deserialization(self._object_ids)
        self._resolved = True

    def __iter__(self) -> "Iterator[pd.DataFrame]":
        for i in range(len(self._object_ids)):
            object_id = self._object_ids[i]
            assert object_id is not None
            data = ray.get(object_id)
            reader = pa.ipc.open_stream(data)
            tb = reader.read_all()
            df: pd.DataFrame = tb.to_pandas()
            yield df
