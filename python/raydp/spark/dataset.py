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

from typing import Callable, List, NoReturn, Optional, Iterable, Union

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyspark.sql as sql
import ray
import ray.util.data as ml_dataset
import ray.util.iter as parallel_it
from ray.util.data import MLDataset
from ray.util.data.interface import _SourceShard

from raydp.utils import divide_blocks


class RecordPiece:
    def __init__(self, row_ids):
        self.row_ids = row_ids

    def read(self, shuffle: bool) -> pd.DataFrame:
        raise NotImplementedError

    def with_row_ids(self, new_row_ids) -> "RecordPiece":
        raise NotImplementedError


class RayObjectPiece(RecordPiece):
    def __init__(self,
                 obj_id: ray.ObjectRef,
                 row_ids: Optional[List[int]]):
        super().__init__(row_ids)
        self.obj_id = obj_id

    def read(self, shuffle: bool) -> pd.DataFrame:
        data = ray.get(self.obj_id)
        reader = pa.ipc.open_stream(data)
        tb = reader.read_all()
        df: pd.DataFrame = tb.to_pandas()
        if self.row_ids:
            df = df.loc[self.row_ids]

        if shuffle:
            df = df.sample(frac=1.0)
        return df

    def with_row_ids(self, new_row_ids) -> "RayObjectPiece":
        return RayObjectPiece(self.obj_id, new_row_ids)


class ParquetPiece(RecordPiece):
    def __init__(self,
                 piece: pq.ParquetDatasetPiece,
                 columns: List[str],
                 partitions: pq.ParquetPartitions,
                 row_ids: Optional[List[int]]):
        super().__init__(row_ids)
        self.piece = piece
        self.columns = columns
        self.partitions = partitions

    def read(self, shuffle: bool) -> pd.DataFrame:
        pdf = self.piece.read(columns=self.columns,
                              use_threads=False,
                              partitions=self.partitions).to_pandas()
        if self.row_ids:
            pdf = pdf.loc[self.row_ids]

        if shuffle:
            pdf = pdf.sample(frac=1.0)
        return pdf

    def with_row_ids(self, new_row_ids) -> "ParquetPiece":
        return ParquetPiece(self.piece, self.columns, self.partitions, new_row_ids)


class RecordBatch(_SourceShard):
    def __init__(self,
                 shard_id: int,
                 prefix: str,
                 record_pieces: List[RecordPiece],
                 shuffle: bool,
                 shuffle_seed: int):
        self._shard_id = shard_id
        self._prefix = prefix
        self.record_pieces = record_pieces
        self.shuffle = shuffle
        self.shuffle_seed = shuffle_seed

    def prefix(self) -> str:
        return self._prefix

    @property
    def shard_id(self) -> int:
        return self._shard_id

    def __iter__(self) -> Iterable[pd.DataFrame]:
        if self.shuffle:
            np.random.seed(self.shuffle_seed)
            np.random.shuffle(self.record_pieces)

        for piece in self.record_pieces:
            yield piece.read(self.shuffle)


class RayRecordBatch(RecordBatch):
    def __init__(self,
                 shard_id: int,
                 prefix: str,
                 record_pieces: List[RecordPiece],
                 shuffle: bool,
                 shuffle_seed: int):
        super().__init__(shard_id, prefix, record_pieces, shuffle, shuffle_seed)
        self.resolved: bool = False

    def resolve(self, timeout: Optional[float] = None) -> NoReturn:
        """
        This is just fetch object from remote object store to local and without deserialization.
        :param timeout: The maximum amount of time in seconds to wait before returning.
        """
        if self.resolved:
            return

        worker = ray.worker.global_worker
        worker.check_connected()
        timeout_ms = int(timeout * 1000) if timeout else -1
        object_ids = [record.obj_id for record in self.record_pieces]
        worker.core_worker.get_objects(object_ids, worker.current_task_id, timeout_ms)
        self.resolved = True


def _save_spark_df_to_object_store(df: sql.DataFrame):
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

    return blocks, block_sizes


def _create_ml_dataset(name: str,
                       record_pieces: List[RecordPiece],
                       record_sizes: List[int],
                       num_shards: int,
                       shuffle: bool,
                       shuffle_seed: int,
                       RecordBatchCls,
                       create_ml_ds_actor_fn) -> MLDataset:
    if shuffle_seed:
        np.random.seed(shuffle_seed)
    else:
        np.random.seed(0)

    # split the piece into num_shards partitions
    divided_blocks = divide_blocks(blocks=record_sizes,
                                   world_size=num_shards,
                                   shuffle=shuffle,
                                   shuffle_seed=shuffle_seed)

    record_batches = []

    for rank, blocks in divided_blocks.items():
        pieces = []
        for index, num_samples in blocks:
            record_size = record_sizes[index]
            piece = record_pieces[index]
            if num_samples != record_size:
                assert num_samples < record_size
                new_row_ids = np.random.choice(
                    record_size, size=num_samples).tolist()
                piece = piece.with_row_ids(new_row_ids)
            pieces.append(piece)
        np.random.shuffle(pieces)
        record_batches.append(RecordBatchCls(shard_id=rank,
                                             prefix=name,
                                             record_pieces=pieces,
                                             shuffle=shuffle,
                                             shuffle_seed=shuffle_seed))

    if create_ml_ds_actor_fn is not None:
        actors = []
        for rank, record_batch in enumerate(record_batches):
            actors.append(create_ml_ds_actor_fn(rank, record_batch))
        it = parallel_it.from_actors(actors, name=name)
    else:
        it = parallel_it.from_iterators(generators=record_batches,
                                        name=name,
                                        repeat=False)
    ds = ml_dataset.from_parallel_iter(
        it, need_convert=False, batch_size=0, repeated=False)
    return ds


class RayMLDataset:
    @staticmethod
    def from_spark(df: sql.DataFrame,
                   num_shards: int,
                   shuffle: bool = True,
                   shuffle_seed: int = None,
                   fs_directory: Optional[str] = None,
                   compression: Optional[str] = None,
                   create_ml_ds_actor_fn: Callable = None) -> MLDataset:
        """ Create a MLDataset from Spark DataFrame

        This method will create a MLDataset from Spark DataFrame.

        :param df: the pyspark.sql.DataFrame
        :param num_shards: the number of shards will be created for the MLDataset
        :param shuffle: whether need to shuffle the blocks when create the MLDataset
        :param shuffle_seed: the shuffle seed, default is 0
        :param fs_directory: an optional distributed file system directory for cache the
            DataFrame. We will write the DataFrame to the given directory with parquet
            format if this is provided. Otherwise, we will write the DataFrame to ray
            object store.
        :param compression: the optional compression for write the DataFrame as parquet
            file. This is only useful when the fs_directory set.
        :param create_ml_ds_actor_fn: a function to create the MLDataset actor, the input is
               the rank_id and the given RecordBatch for the rank.
        :return: a MLDataset
        """
        df = df.repartition(num_shards)
        if fs_directory is None:
            # fs_directory has not provided, we save the Spark DataFrame to ray object store
            blocks, block_sizes = _save_spark_df_to_object_store(df)
            record_pieces = [RayObjectPiece(obj, None) for obj in blocks]

            return _create_ml_dataset("from_spark", record_pieces, block_sizes, num_shards,
                                      shuffle, shuffle_seed, RayRecordBatch,
                                      create_ml_ds_actor_fn)
        else:
            # fs_directory has provided, we write the Spark DataFrame as Parquet files
            df.write.parquet(fs_directory, compression=compression)
            # create the MLDataset from the parquet file
            ds = RayMLDataset.from_parquet(fs_directory, num_shards, shuffle, shuffle_seed,
                                           create_ml_ds_actor_fn=create_ml_ds_actor_fn)
            return ds

    @staticmethod
    def from_parquet(paths: Union[str, List[str]],
                     num_shards: int,
                     shuffle: bool = True,
                     shuffle_seed: int = None,
                     columns: Optional[List[str]] = None,
                     create_ml_ds_actor_fn: Callable = None,
                     **kwargs) -> MLDataset:
        ds = pq.ParquetDataset(paths, **kwargs)
        pieces = ds.pieces
        record_pieces = []
        record_sizes = []

        for piece in pieces:
            meta_data = piece.get_metadata().to_dict()
            num_row_groups = meta_data["num_row_groups"]
            row_groups = meta_data["row_groups"]
            for i in range(num_row_groups):
                parquet_ds_piece = pq.ParquetDatasetPiece(piece.path, piece.open_file_func,
                                                          piece.file_options, i,
                                                          piece.partition_keys)
                # row_ids will be set later
                record_pieces.append(ParquetPiece(piece=parquet_ds_piece,
                                                  columns=columns,
                                                  partitions=ds.partitions,
                                                  row_ids=None))
                record_sizes.append(row_groups[i]["num_rows"])

        return _create_ml_dataset("from_parquet", record_pieces, record_sizes, num_shards,
                                  shuffle, shuffle_seed, RecordBatch, create_ml_ds_actor_fn)


def create_ml_dataset_from_spark(df: sql.DataFrame,
                                 num_shards: int,
                                 shuffle: bool,
                                 shuffle_seed: int,
                                 fs_directory: Optional[str] = None,
                                 compression: Optional[str] = None) -> MLDataset:
    return RayMLDataset.from_spark(
        df, num_shards, shuffle, shuffle_seed, fs_directory, compression)
