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

from typing import List, NoReturn, Optional

import pandas as pd
import pyarrow as pa
import pyspark.sql as sql
import ray
import ray.util.data as ml_dataset
import ray.util.iter as parallel_it
from ray.util.data import MLDataset
from ray.util.data.interface import _SourceShard

from raydp.utils import divide_blocks


def create_ml_dataset_from_spark(df: sql.DataFrame,
                                 num_shards: int,
                                 batch_size: int,
                                 fs_directory: Optional[str] = None,
                                 compression: Optional[str] = None) -> MLDataset:
    """ Create a MLDataset from Spark DataFrame

    This method will create a MLDataset from Spark DataFrame.

    :param df: the pyspark.sql.DataFrame
    :param num_shards: the number of shards will be created for the MLDataset
    :param batch_size: the batch size for the MLDataset
    :param fs_directory: an optional distributed file system directory for cache the
           DataFrame. We will write the DataFrame to the given directory with parquet
           format if this is provided. Otherwise, we will write the DataFrame to ray
           object store.
    :param compression: the optional compression for write the DataFrame as parquet
           file. This is only useful when the fs_directory set.
    :return: a MLDataset
    """
    df = df.repartition(num_shards)
    if fs_directory is None:
        # fs_directory has not provided, we save the Spark DataFrame to ray object store
        record_batch_set = _save_spark_df_to_object_store(df, num_shards)
        # TODO: we should specify the resource spec for each shard
        it = parallel_it.from_iterators(generators=record_batch_set,
                                        name="Spark DataFrame",
                                        repeat=False)
        ds = ml_dataset.from_parallel_iter(
            it, need_convert=False, batch_size=batch_size, repeated=False)
        return ds
    else:
        # fs_directory has provided, we write the Spark DataFrame as Parquet files
        df.write.parquet(fs_directory, compression=compression)
        # create the MLDataset from the parquet file
        ds = ml_dataset.read_parquet(fs_directory, num_shards)
        return ds


def _save_spark_df_to_object_store(df: sql.DataFrame,
                                   num_shards: int) -> List["RecordBatchShard"]:
    # call java function from python
    jvm = df.sql_ctx.sparkSession.sparkContext._jvm
    jdf = df._jdf
    object_store_writer = jvm.org.apache.spark.sql.raydp.ObjectStoreWriter(jdf)
    records = object_store_writer.save()

    worker = ray.worker.global_worker

    blocks: List[ray.ObjectRef] = []
    block_sizes: List[int] = []
    for record in records:
        owner_address = record.ownerAddress()
        object_ref = ray.ObjectRef(record.objectId())
        num_records = record.numRecords()
        # Register the ownership of the ObjectRef
        worker.core_worker.deserialize_and_register_object_ref(
            object_ref.binary(), ray.ObjectRef.nil(), owner_address)

        blocks.append(object_ref)
        block_sizes.append(num_records)

    divided_blocks = divide_blocks(block_sizes, num_shards)
    record_batch_set: List[RecordBatchShard] = []
    for i in range(num_shards):
        indexes = divided_blocks[i]
        object_ids = [blocks[index] for index in indexes]
        record_batch_set.append(RecordBatchShard(i, object_ids))
    return record_batch_set


class RecordBatchShard(_SourceShard):
    def __init__(self,
                 shard_id: int,
                 object_ids: List[ray.ObjectRef]):
        if not isinstance(object_ids, list):
            object_ids = [object_ids]
        self._object_ids: List[ray.ObjectRef] = object_ids
        self._shard_id = shard_id
        self._resolved: bool = False

    def resolve(self, timeout: Optional[float] = None) -> NoReturn:
        """
        This is just fetch object from remote object store to local and without deserialization.
        :param timeout: The maximum amount of time in seconds to wait before returning.
        """
        if self._resolved:
            return

        worker = ray.worker.global_worker
        worker.check_connected()
        timeout_ms = int(timeout * 1000) if timeout else -1
        worker.core_worker.get_objects(self._object_ids, worker.current_task_id, timeout_ms)
        self._resolved = True

    def prefix(self) -> str:
        return "SparkShard"

    @property
    def shard_id(self) -> int:
        return self._shard_id

    def __iter__(self):
        for obj in self._object_ids:
            assert obj is not None
            data = ray.get(obj)
            reader = pa.ipc.open_stream(data)
            tb = reader.read_all()
            df: pd.DataFrame = tb.to_pandas()
            yield df
