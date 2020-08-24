import glob
from typing import Any, Callable, Dict, List, NoReturn, Optional

import pandas as pd
import pyarrow as pa
import pyarrow.plasma as plasma
import pyspark
import ray
from pyarrow.plasma import PlasmaClient
from pyspark.sql.session import SparkSession

from raydp.spark.resource_manager.exchanger import SharedDataset
from raydp.spark.resource_manager.ray.ray_cluster_master import RayClusterMaster, RAYDP_CP
from raydp.spark.resource_manager.spark_cluster import SparkCluster


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

    def save_to_ray(self, df: pyspark.sql.DataFrame) -> SharedDataset:
        # call java function from python
        sql_context = df.sql_ctx
        jvm = sql_context.sparkSession.sparkContext._jvm
        jdf = df._jdf
        object_store_writer = jvm.org.apache.spark.sql.raydp.ObjectStoreWriter(jdf)
        records = object_store_writer.save()

        worker = ray.worker.global_worker

        object_ids = []
        sizes = []
        for record in records:
            owner_address = record.ownerAddress()
            object_id = ray.ObjectID(record.objectId())
            num_records = record.numRecords()
            # Register the ownership of the ObjectRef
            worker.core_worker.deserialize_and_register_object_ref(
                object_id.binary(), ray.ObjectRef.nil(), owner_address)

            object_ids.append(object_id)
            sizes.append(num_records)

        return RayClusterSharedDataset(object_ids, sizes)

    def stop(self):
        if self._spark_session is not None:
            self._spark_session.stop()
            self._spark_session = None

        if self._app_master_bridge is not None:
            self._app_master_bridge.stop()
            self._app_master_bridge = None


class RayClusterSharedDataset(SharedDataset):
    def __init__(self,
                 object_ids: List[ray.ObjectID],
                 sizes: List[int],
                 plasma_store_socket_name: str = None,
                 index_map_func: Callable = lambda x: x):
        self._object_ids: List[ray.ObjectID] = object_ids
        self._sizes = sizes
        self._plasma_store_socket_name = plasma_store_socket_name
        self._index_map_func = index_map_func
        in_ray_worker: bool = ray.is_initialized()
        self._get_data_func = ray.get
        if not in_ray_worker:
            # if the current process is not a Ray worker, the
            # plasma_store_socket_name must be set
            assert plasma_store_socket_name is not None, "plasma_store_socket_name must be set"
            plasma_client: Optional[PlasmaClient] = plasma.connect(plasma_store_socket_name)

            def get_by_plasma(object_id: ray.ObjectID):
                plasma_object_id = plasma.ObjectID(object_id.binary())
                # this should be really faster becuase of zero copy
                data = plasma_client.get_buffers([plasma_object_id])[0]
                return data

            self._get_data_func = get_by_plasma

    def set_plasma_store_socket_name(self, path: str) -> NoReturn:
        self._plasma_store_socket_name = path

    def total_size(self) -> int:
        return sum(self._sizes)

    def partition_sizes(self) -> List[int]:
        return self._sizes

    def resolve(self, timeout=None) -> bool:
        self._fetch_objects_without_deserialization(self._object_ids, timeout)
        return True

    def subset(self, indexes: List[int]) -> 'SharedDataset':
        index_map = dict(zip(indexes, range(len(indexes))))

        def index_map_func(i: int) -> int:
            assert i in index_map
            return index_map[i]

        subset_object_ids = [self._object_ids[i] for i in indexes]
        subset_sizes = [self._sizes[i] for i in indexes]
        return RayClusterSharedDataset(
            subset_object_ids, subset_sizes, self._plasma_store_socket_name, index_map_func)

    def __getitem__(self, index: int) -> pd.DataFrame:
        index = self._index_map_func(index)
        object_id = self._object_ids[index]
        assert object_id is not None
        data = self._get_data_func(object_id)
        reader = pa.ipc.open_stream(data)
        tb = reader.read_all()
        df = tb.to_pandas()
        return df

    @classmethod
    def _custom_deserialize(cls,
                            object_ids: List[ray.ObjectID],
                            sizes: List[int],
                            plasma_store_socket_name: str,
                            index_map_func: Callable):
        instance = cls(object_ids, sizes, plasma_store_socket_name, index_map_func)
        return instance

    def __reduce__(self):
        return (RayClusterSharedDataset._custom_deserialize,
                (self._object_ids, self._sizes, self._plasma_store_socket_name,
                 self._index_map_func))
