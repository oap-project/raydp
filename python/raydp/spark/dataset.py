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

import logging
import uuid
from typing import Callable, Dict, List, NoReturn, Optional, Iterable, Union

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyspark.sql as sql
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import StructType
from pyspark.sql.pandas.types import from_arrow_type
import ray
from ray.data import Dataset, from_arrow_refs
from ray.types import ObjectRef
import ray.util.iter as parallel_it
from ray._private.client_mode_hook import client_mode_wrap
try:
    import ray.util.data as ml_dataset
    from ray.util.data import MLDataset
    from ray.util.data.interface import _SourceShard
    HAS_MLDATASET = True
except ModuleNotFoundError:
    # Ray MLDataset is removed in Ray 2.0
    HAS_MLDATASET = False
from raydp.spark.parallel_iterator_worker import ParallelIteratorWorkerWithLen
from raydp.utils import divide_blocks


logger = logging.getLogger(__name__)


class RecordPiece:
    def __init__(self, row_ids, num_rows: int):
        self.row_ids = row_ids
        self.num_rows = num_rows

    def read(self, shuffle: bool) -> pd.DataFrame:
        raise NotImplementedError

    def with_row_ids(self, new_row_ids) -> "RecordPiece":
        raise NotImplementedError

    def __len__(self):
        """Return the number of rows"""
        return self.num_rows


class RayObjectPiece(RecordPiece):
    def __init__(self,
                 obj_id: ray.ObjectRef,
                 row_ids: Optional[List[int]],
                 num_rows: int):
        super().__init__(row_ids, num_rows)
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
        """chang the num_rows to the length of new_row_ids. Keep the original size if
        the new_row_ids is None.
        """

        if new_row_ids:
            num_rows = len(new_row_ids)
        else:
            num_rows = self.num_rows

        return RayObjectPiece(self.obj_id, new_row_ids, num_rows)


class ParquetPiece(RecordPiece):
    def __init__(self,
                 piece: pq.ParquetDatasetPiece,
                 columns: List[str],
                 partitions: pq.ParquetPartitions,
                 row_ids: Optional[List[int]],
                 num_rows: int):
        super().__init__(row_ids, num_rows)
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
        """
        chang the num_rows to the length of new_row_ids. Keep the original size if
        the new_row_ids is None.
        """
        if new_row_ids:
            num_rows = len(new_row_ids)
        else:
            num_rows = self.num_rows
        return ParquetPiece(self.piece, self.columns, self.partitions, new_row_ids, num_rows)


@client_mode_wrap
def _register_objects(records):
    worker = ray.worker.global_worker
    blocks: List[ray.ObjectRef] = []
    block_sizes: List[int] = []
    for obj_id, owner, num_record in records:
        object_ref = ray.ObjectRef(obj_id)
        # Register the ownership of the ObjectRef
        worker.core_worker.deserialize_and_register_object_ref(
            object_ref.binary(), ray.ObjectRef.nil(), owner, "")
        blocks.append(object_ref)
        block_sizes.append(num_record)
    return blocks, block_sizes

def _save_spark_df_to_object_store(df: sql.DataFrame, use_batch: bool = True,
                                   _use_owner: bool = False):
    # call java function from python
    jvm = df.sql_ctx.sparkSession.sparkContext._jvm
    jdf = df._jdf
    object_store_writer = jvm.org.apache.spark.sql.raydp.ObjectStoreWriter(jdf)

    if _use_owner is True:
        records = object_store_writer.save(use_batch, RAYDP_OBJ_HOLDER_NAME)
    else:
        records = object_store_writer.save(use_batch, "")

    record_tuples = [(record.objectId(), record.ownerAddress(), record.numRecords())
                        for record in records]
    blocks, block_sizes = _register_objects(record_tuples)

    if _use_owner is True:
        holder = ray.get_actor(RAYDP_OBJ_HOLDER_NAME)
        df_id = uuid.uuid4()
        ray.get(holder.add_objects.remote(df_id, blocks))

    return blocks, block_sizes


def spark_dataframe_to_ray_dataset(df: sql.DataFrame,
                                   parallelism: Optional[int] = None,
                                   _use_owner: bool = False):
    num_part = df.rdd.getNumPartitions()
    if parallelism is not None:
        if parallelism < num_part:
            df = df.coalesce(parallelism)
        elif parallelism > num_part:
            df = df.repartition(parallelism)
    blocks, _ = _save_spark_df_to_object_store(df, False, _use_owner)
    return from_arrow_refs(blocks)

@ray.remote
class RayDPConversionHelper():
    def __init__(self):
        self.objects = {}
        self.this_actor_id = None

    def add_objects(self, timestamp, objects):
        self.objects[timestamp] = objects

    def get_object(self, timestamp, idx):
        return self.objects[timestamp][idx]

    def get_ray_address(self):
        return ray.worker.global_worker.node.address

    def terminate(self):
        ray.actor.exit_actor()

    def get_actor_id(self):
        self.this_actor_id = ray.get_runtime_context().actor_id
        return self.this_actor_id

RAYDP_OBJ_HOLDER_NAME = "raydp_obj_holder"

def _convert_by_udf(spark: sql.SparkSession,
                    blocks: List[ObjectRef],
                    locations: List[bytes],
                    schema: StructType) -> DataFrame:
    holder = ray.get_actor(RAYDP_OBJ_HOLDER_NAME)
    df_id = uuid.uuid4()
    ray.get(holder.add_objects.remote(df_id, blocks))
    jvm = spark.sparkContext._jvm
    object_store_reader = jvm.org.apache.spark.sql.raydp.ObjectStoreReader
    # create the rdd then dataframe to utilize locality
    jdf = object_store_reader.createRayObjectRefDF(spark._jsparkSession, locations)
    current_namespace = ray.get_runtime_context().namespace
    ray_address = ray.get(holder.get_ray_address.remote())
    blocks_df = DataFrame(jdf, spark._wrapped)
    def _convert_blocks_to_dataframe(blocks):
        # connect to ray
        if not ray.is_initialized():
            ray.init(address=ray_address,
                     namespace=current_namespace,
                     logging_level=logging.WARN)
        obj_holder = ray.get_actor(RAYDP_OBJ_HOLDER_NAME)
        for block in blocks:
            dfs = []
            for idx in block["idx"]:
                ref = ray.get(obj_holder.get_object.remote(df_id, idx))
                data = ray.get(ref)
                dfs.append(data.to_pandas())
            yield pd.concat(dfs)
    df = blocks_df.mapInPandas(_convert_blocks_to_dataframe, schema)
    return df

def _convert_by_rdd(spark: sql.SparkSession,
                    blocks: Dataset,
                    locations: List[bytes],
                    schema: StructType) -> DataFrame:
    object_ids = [block.binary() for block in blocks]
    schema_str = schema.json()
    jvm = spark.sparkContext._jvm
    # create rdd in java
    rdd = jvm.org.apache.spark.rdd.RayDatasetRDD(spark._jsc, object_ids, locations)
    # convert the rdd to dataframe
    object_store_reader = jvm.org.apache.spark.sql.raydp.ObjectStoreReader
    jdf = object_store_reader.RayDatasetToDataFrame(spark._jsparkSession, rdd, schema_str)
    return DataFrame(jdf, spark._wrapped)

@client_mode_wrap
def get_locations(blocks):
    core_worker = ray.worker.global_worker.core_worker
    return [
        core_worker.get_owner_address(block)
        for block in blocks
    ]

def ray_dataset_to_spark_dataframe(spark: sql.SparkSession,
                                   arrow_schema: "pa.lib.Schema",
                                   blocks: List[ObjectRef],
                                   locations = None) -> DataFrame:
    locations = get_locations(blocks)
    if not isinstance(arrow_schema, pa.lib.Schema):
        raise RuntimeError(f"Schema is {type(arrow_schema)}, required pyarrow.lib.Schema. \n" \
                           f"to_spark does not support converting non-arrow ray datasets.")
    schema = StructType()
    for field in arrow_schema:
        schema.add(field.name, from_arrow_type(field.type), nullable=field.nullable)
    #TODO how to branch on type of block?
    sample = ray.get(blocks[0])
    if isinstance(sample, bytes):
        return _convert_by_rdd(spark, blocks, locations, schema)
    elif isinstance(sample, pa.Table):
        return _convert_by_udf(spark, blocks, locations, schema)
    else:
        raise RuntimeError("ray.to_spark only supports arrow type blocks")

if HAS_MLDATASET:
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

        def __len__(self):
            return sum([len(piece) for piece in self.record_pieces])


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


    def _create_ml_dataset(name: str,
                        record_pieces: List[RecordPiece],
                        record_sizes: List[int],
                        num_shards: int,
                        shuffle: bool,
                        shuffle_seed: int,
                        RecordBatchCls,
                        node_hints: List[str] = None) -> MLDataset:
        if node_hints is not None:
            assert num_shards % len(node_hints) == 0,\
                (f"num_shards: {num_shards} should be a multiple"
                 f" of length of node_hints: {node_hints}")
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

            if shuffle:
                np.random.shuffle(pieces)
            record_batches.append(RecordBatchCls(shard_id=rank,
                                                prefix=name,
                                                record_pieces=pieces,
                                                shuffle=shuffle,
                                                shuffle_seed=shuffle_seed))

        worker_cls = ray.remote(ParallelIteratorWorkerWithLen)
        if node_hints is not None:
            actors = []
            multiplier = num_shards // len(node_hints)
            resource_keys = [f"node:{node_hints[i // multiplier]}" for i in range(num_shards)]
            for g, resource_key in zip(record_batches, resource_keys):
                actor = worker_cls.options(resources={resource_key: 0.01}).remote(g, False, len(g))
                actors.append(actor)
        else:
            worker_cls = ray.remote(ParallelIteratorWorkerWithLen)
            actors = [worker_cls.remote(g, False, len(g)) for g in record_batches]

        it = parallel_it.from_actors(actors, name)
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
                    node_hints: List[str] = None) -> MLDataset:
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
            :param node_hints: the node hints to create MLDataset actors
            :return: a MLDataset
            """
            df = df.repartition(num_shards)
            if fs_directory is None:
                # fs_directory has not provided, we save the Spark DataFrame to ray object store
                blocks, block_sizes = _save_spark_df_to_object_store(df)
                record_pieces = [RayObjectPiece(obj, None, num_rows)
                                for obj, num_rows in zip(blocks, block_sizes)]

                return _create_ml_dataset("from_spark", record_pieces, block_sizes, num_shards,
                                        shuffle, shuffle_seed, RayRecordBatch,
                                        node_hints)
            else:
                # fs_directory has provided, we write the Spark DataFrame as Parquet files
                df.write.parquet(fs_directory, compression=compression)
                # create the MLDataset from the parquet file
                ds = RayMLDataset.from_parquet(
                    fs_directory, num_shards, shuffle, shuffle_seed, node_hints)
                return ds

        @staticmethod
        def from_parquet(paths: Union[str, List[str]],
                        num_shards: int,
                        shuffle: bool = True,
                        shuffle_seed: int = None,
                        columns: Optional[List[str]] = None,
                        node_hints: List[str] = None,
                        extra_parquet_arguments: Dict = None) -> MLDataset:
            """ Create a MLDataset from Parquet files.

            :param paths: the parquet files path
            :param num_shards: the number of shards will be created for the MLDataset
            :param shuffle: whether need to shuffle the blocks when create the MLDataset
            :param shuffle_seed: the shuffle seed, default is 0
            :param columns: the columns that need to read
            :param node_hints: the node hints to create MLDataset actors
            :param extra_parquet_arguments: the extra arguments need to pass into the parquet file
                reading
            :return: a MLDataset
            """
            if not extra_parquet_arguments:
                extra_parquet_arguments = {}
            ds = pq.ParquetDataset(paths, **extra_parquet_arguments)
            pieces = ds.pieces
            record_pieces = []
            record_sizes = []

            for piece in pieces:
                meta_data = piece.get_metadata().to_dict()
                num_row_groups = meta_data["num_row_groups"]
                row_groups = meta_data["row_groups"]
                for i in range(num_row_groups):
                    num_rows = row_groups[i]["num_rows"]
                    parquet_ds_piece = pq.ParquetDatasetPiece(piece.path, piece.open_file_func,
                                                            piece.file_options, i,
                                                            piece.partition_keys)
                    # row_ids will be set later
                    record_pieces.append(ParquetPiece(piece=parquet_ds_piece,
                                                    columns=columns,
                                                    partitions=ds.partitions,
                                                    row_ids=None,
                                                    num_rows=num_rows))
                    record_sizes.append(num_rows)

            return _create_ml_dataset("from_parquet", record_pieces, record_sizes, num_shards,
                                    shuffle, shuffle_seed, RecordBatch, node_hints)

        @staticmethod
        def to_torch(
                ds: MLDataset,
                world_size: int,
                world_rank: int,
                batch_size: int,
                collate_fn: Callable,
                shuffle: bool = False,
                shuffle_seed: int = None,
                local_rank: int = -1,
                prefer_node: str = None,
                prefetch: bool = False):
            """
            Create DataLoader from a MLDataset
            :param ds: the MLDataset
            :param world_size: the world_size of distributed model training
            :param world_rank: create the DataLoader for the given world_rank
            :param batch_size: the batch_size of the DtaLoader
            :param collate_fn: the collate_fn that create tensors from a pandas DataFrame
            :param shuffle: whether shuffle each batch of data
            :param shuffle_seed: the shuffle seed
            :param local_rank: the node local rank. It must be provided if prefer_node is
                not None.
            :param prefer_node: the prefer node for create the MLDataset actor
            :param prefetch: prefetch the data of DataLoader with one thread
            :return: a pytorch DataLoader
            """
            # pylint: disable=C0415
            import torch
            from raydp.torch.torch_ml_dataset import PrefetchedDataLoader, TorchMLDataset

            num_shards = ds.num_shards()
            assert num_shards % world_size == 0, \
                (f"The number shards of MLDataset({ds}) should be a multiple of "
                f"world_size({world_size})")
            multiplier = num_shards // world_size

            selected_ds = None
            if prefer_node is not None:
                assert 0 <= local_rank < world_size

                # get all actors
                # there should be only one actor_set because of select_shards() is not allowed
                # after union()

                def location_check(actor):
                    address = ray.actors(actor._actor_id.hex())["Address"]["IPAddress"]
                    return address == prefer_node

                actors = ds.actor_sets[0].actors
                actor_indexes = [i for i, actor in enumerate(actors) if location_check(actor)]
                if len(actor_indexes) % multiplier != 0:
                    selected_ds = None
                    logger.warning(f"We could not find enough shard actor in prefer "
                                f"node({prefer_node}), fail back to normal select_shards(). "
                                f"Found: ({actor_indexes}) which length is not multiple of "
                                f"num_shards({num_shards}) // world_size({world_size}).")
                else:
                    shard_ids = actor_indexes[local_rank: local_rank + multiplier]
                    selected_ds = ds.select_shards(shard_ids)

            if selected_ds is None:
                shard_ids = []
                i = world_rank
                step = world_size
                while i < num_shards:
                    shard_ids.append(i)
                    i += step
                selected_ds = ds.select_shards(shard_ids)

            selected_ds = selected_ds.batch(batch_size)
            torch_ds = TorchMLDataset(selected_ds, collate_fn, shuffle, shuffle_seed)
            data_loader = torch.utils.data.DataLoader(dataset=torch_ds,
                                                    batch_size=None,
                                                    batch_sampler=None,
                                                    shuffle=False,
                                                    num_workers=0,
                                                    collate_fn=None,
                                                    pin_memory=False,
                                                    drop_last=False,
                                                    sampler=None)
            if prefetch:
                data_loader = PrefetchedDataLoader(data_loader)
            return data_loader


    def create_ml_dataset_from_spark(df: sql.DataFrame,
                                    num_shards: int,
                                    shuffle: bool,
                                    shuffle_seed: int,
                                    fs_directory: Optional[str] = None,
                                    compression: Optional[str] = None,
                                    node_hints: List[str] = None) -> MLDataset:
        return RayMLDataset.from_spark(
            df, num_shards, shuffle, shuffle_seed, fs_directory, compression, node_hints)
