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

import copy
import pickle
from collections import defaultdict
from typing import Any, Callable, Dict, Iterator, List, NoReturn, Optional, Tuple

import numpy as np
import pandas as pd
import pyarrow.plasma as plasma
import pyspark
import ray
from pyarrow.plasma import PlasmaClient
from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType

from raydp.ray_cluster_resources import ClusterResources
from raydp.spark.resource_manager.exchanger import SharedDataset
from raydp.spark.resource_manager.spark_cluster import SparkCluster
from raydp.spark.resource_manager.standalone.block_holder import BlockHolder, BlockHolderActorHandlerWrapper
from raydp.spark.resource_manager.standalone.standalone_cluster_master import StandaloneMasterService
from raydp.spark.resource_manager.standalone.standalone_cluster_worker import StandaloneWorkerService
from raydp.spark.utils import get_node_address

_global_broadcasted = {}
# TODO: better way to stop and clean data holder
_global_block_holder: Dict[str, BlockHolderActorHandlerWrapper] = {}


default_config = {
    "spark.sql.execution.arrow.enabled": "true"
}


class StandaloneCluster(SparkCluster):
    def __init__(self,
                 spark_home: str,
                 master_resources: Dict[str, float] = {"num_cpus": 0},
                 master_port: int = 8080,
                 master_webui_port: Any = None,
                 master_properties: Dict[str, str] = None):
        assert ray.is_initialized()

        super().__init__(master_resources)

        self._ray_redis_address = ray.worker._global_node.redis_address
        self._ray_redis_password = ray.worker._global_node.redis_password

        self._spark_home = spark_home
        self._master_port = master_port
        self._master_webui_port = master_webui_port
        self._master_properties = master_properties
        self._master_resources = master_resources

        self._master = None
        self._workers = []
        self._master_url = None

        self._node_selected = set()

        self._stopped = False

    def _resource_check(self, resources: Dict[str, float]) -> str:
        # check whether this is any node could satisfy the master service requirement
        satisfied = ClusterResources.satisfy(resources)
        satisfied = [node for node in satisfied if node not in self._node_selected]
        if not satisfied:
            raise Exception("There is not any node can satisfy the service resources "
                            f"requirement, request: {resources}")

        choosed = np.random.choice(satisfied)
        assert choosed not in self._node_selected
        return choosed

    def _set_up_master(self, resources: Dict[str, float], kwargs: Dict[str, str]):
        self._resource_check(resources)
        # set up master
        master_remote = ray.remote(**resources)(StandaloneMasterService)
        self._master = master_remote.remote(
            self._spark_home, self._master_port, self._master_webui_port, self._master_properties)
        # start up master
        error_msg = ray.get(self._master.start_up.remote())
        if not error_msg:
            # get master url after master start up
            self._master_url = ray.get(self._master.get_master_url.remote())
        else:
            ray.get(self._master.stop.remote())
            ray.kill(self._master)
            self._master = None
            raise Exception(error_msg)

    def _set_up_worker(self, resources: Dict[str, float], kwargs: Dict[str, str]):
        if self._master is None:
            self._set_up_master(self._master_resources, None)

        # set up workers
        total_alive_nodes = ClusterResources.total_alive_nodes()
        if total_alive_nodes <= self._num_nodes:
            raise Exception("Don't have enough nodes,"
                            f"available: {total_alive_nodes}, existed: {self._num_nodes}")

        resources_copy: Dict[str, Any] = copy.deepcopy(resources)
        choosed = self._resource_check(resources_copy)
        resources_copy["resources"] = {choosed: 0.01}

        port = kwargs.get("port", None)
        webui_port = kwargs.get("webui_port", None)
        properties = kwargs.get("properties", None)
        worker_remote = ray.remote(**resources_copy)(StandaloneWorkerService)
        worker = worker_remote.remote(master_url=self._master_url,
                                      cores=resources_copy["num_cpus"],
                                      memory=resources_copy["memory"],
                                      spark_home=self._spark_home,
                                      port=port,
                                      webui_port=webui_port,
                                      properties=properties)
        # startup worker
        error_msg = ray.get(worker.start_up.remote())
        if not error_msg:
            self._node_selected.add(choosed)
            if choosed not in _global_block_holder:
                # set up block holder if has not set up
                block_holder = BlockHolder.options(resources={choosed: 0.01})\
                                          .remote(node_label=choosed,
                                                  concurrent_save=resources_copy["num_cpus"])
                _global_block_holder[choosed] = BlockHolderActorHandlerWrapper(block_holder)

            self._workers.append(worker)
            self._num_nodes += 1
        else:
            ray.get(worker.stop.remote())
            ray.kill(worker)
            raise Exception(error_msg)

    def get_cluster_url(self) -> str:
        return self._master_url

    def get_spark_session(self,
                          app_name: str,
                          num_executors: int,
                          executor_cores: int,
                          executor_memory: int,
                          extra_conf: Dict[str, str] = None) -> pyspark.sql.SparkSession:

        if self._master is None:
            # this means we will use the executors setting for the workers setting too
            # setup the cluster.
            master_resources = (self._master_resources
                                if self._master_resources is not None else {"num_cpus": 0})
            self._set_up_master(resources=master_resources, kwargs=None)
            worker_resources = {"num_cpus": executor_cores, "memory": executor_memory}
            for _ in range(num_executors):
                self.add_worker(worker_resources)

        conf = default_config
        conf.update(extra_conf)

        builder = SparkSession.builder.appName(app_name).master(self.get_cluster_url())

        builder.config("spark.executor.instances", num_executors)
        builder.config("spark.executor.cores", executor_cores)
        builder.config("spark.executor.memory", executor_memory)

        for k, v in conf.items():
            builder.config(k, v)

        spark = builder.getOrCreate()

        # broadcast redis address and password
        global _global_broadcasted
        value = {"address": self._ray_redis_address, "password": self._ray_redis_password}
        _global_broadcasted["redis"] = spark.sparkContext.broadcast(value)

        return spark

    def save_to_ray(self, df: pyspark.sql.DataFrame) -> SharedDataset:
        return_type = StructType()
        return_type.add(StructField("node_label", StringType(), True))
        return_type.add(StructField("fetch_index", IntegerType(), False))
        return_type.add(StructField("size", LongType(), False))

        @pandas_udf(return_type)
        def save(batch_iter: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
            if not ray.is_initialized():
                redis_config = {}
                broadcasted = _global_broadcasted["redis"].value
                redis_config["address"] = broadcasted["address"]
                redis_config["password"] = broadcasted["password"]

                local_address = get_node_address()

                ray.init(address=redis_config["address"],
                         node_ip_address=local_address,
                         redis_password=redis_config["password"])

            node_label = f"node:{ray.services.get_node_ip_address()}"
            block_holder = _global_block_holder[node_label]
            object_ids = []
            sizes = []
            for pdf in batch_iter:
                obj = ray.put(pickle.dumps(pdf))
                # TODO: register object in batch
                object_ids.append(block_holder.register_object_id.remote([obj]))
                sizes.append(len(pdf))

            indexes = ray.get(object_ids)
            result_dfs = []
            for index, block_size in zip(indexes, sizes):
                result_dfs.append(
                    pd.DataFrame({"node_label": [node_label],
                                  "fetch_index": [index],
                                  "size": [block_size]}))
            return iter(result_dfs)

        results = df.mapInPandas(save).collect()
        fetch_indexes = []
        block_holder_mapping = {}
        block_sizes = []
        for row in results:
            fetch_indexes.append((row["node_label"], row["fetch_index"]))
            block_sizes.append(row["size"])
            if row["node_label"] not in block_holder_mapping:
                block_holder_mapping[row["node_label"]] = _global_block_holder[row["node_label"]]
        return StandaloneClusterSharedDataset(fetch_indexes, block_sizes, block_holder_mapping)

    def stop(self):
        if not self._stopped:
            # TODO: wrap SparkSession to remove this
            _global_broadcasted.clear()
            # stop master
            if self._master:
                ray.get(self._master.stop.remote())

            # stop workers
            if self._workers:
                ray.get([worker.stop.remote() for worker in self._workers])

            # kill master actor
            ray.kill(self._master)
            # kill worker actors
            for worker in self._workers:
                ray.kill(worker)

            self._master = None
            self._workers = []
            self._master_url = None
            self._stopped = True


class StandaloneClusterSharedDataset(SharedDataset):
    """
    A list of fetch index, and each fetch index is wrapper into a Block.

    The workflow of this class:
       1. block_set = BlockSet()  # create instance
       2. block_set.append() or hold_df.append_batch() # add fetch_index data
       3. block_set.resolve()  # Resolve the BlockSet and can't add data again after resolve.
       4. block_set[0].get() # get the underlying data which should be a pandas.DataFrame
    """
    def __init__(self,
                 fetch_indexes: List[Tuple[str, int]],
                 block_sizes: List[int],
                 block_holder_mapping: Dict[str, BlockHolderActorHandlerWrapper],
                 plasma_store_socket_name: str = None):
        assert len(fetch_indexes) == len(block_sizes), \
            "The length of fetch_indexes and block_sizes should be equalled"
        self._fetch_indexes: List[Tuple[str, int]] = fetch_indexes
        self._block_sizes = block_sizes
        self._total_size = sum(self._block_sizes)
        self._block_holder_mapping = block_holder_mapping

        self._resolved = False
        self._resolved_block: Dict[int, ray.ObjectID] = {}

        self._plasma_store_socket_name = plasma_store_socket_name
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

    def total_size(self) -> int:
        return self._total_size

    def partition_sizes(self) -> List[int]:
        return self._block_sizes

    def resolve(self, timeout=None) -> bool:
        if self._resolved:
            return True

        indexes = range(len(self._fetch_indexes))

        grouped = defaultdict(lambda: [])
        label_to_indexes = defaultdict(lambda: [])
        succeed = {}
        # group indices by block holder
        for i in indexes:
            label, index = self._fetch_indexes[i]
            grouped[label].append(index)
            label_to_indexes[label].append(i)

        for label in grouped:
            holder = self._block_holder_mapping.get(label, None)
            assert holder, f"Can't find the DataHolder for the label: {label}"
            object_ids = ray.get(holder.get_object.remote(grouped[label]))
            try:
                # just trigger object transfer without object deserialization.
                self._fetch_objects_without_deserialization(object_ids, timeout)
            except Exception as exp:
                # deserialize or ray.get failed, we should decrease the reference
                for resolved_label in succeed:
                    ray.get(holder.remove_object_ids.remote(grouped[resolved_label]))
                raise exp

            succeed[label] = object_ids

        for label in succeed:
            data = succeed[label]
            ins = label_to_indexes[label]
            for i, d in zip(ins, data):
                self._resolved_block[i] = d

        self._resolved = True
        return True

    def set_resolved_block(self, resolved_block: Dict[int, ray.ObjectID]) -> NoReturn:
        if self._resolved:
            raise Exception("Current dataset has been resolved, you can't set the resolved blocks")
        self._resolved = True
        self._resolved_block = resolved_block

    def subset(self, indexes: List[int]) -> 'SharedDataset':
        subset_fetch_indexes: List[Tuple[str, int]] = []
        subset_block_holder_mapping: Dict[str, BlockHolderActorHandlerWrapper] = {}
        for i in indexes:
            node_label = self._fetch_indexes[i][0]
            subset_fetch_indexes.append(self._fetch_indexes[i])
            if node_label not in subset_block_holder_mapping:
                subset_block_holder_mapping[node_label] = self._block_holder_mapping[node_label]
        subset_block_sizes: List[int] = [self._block_sizes[i] for i in indexes]
        return StandaloneClusterSharedDataset(
            subset_fetch_indexes, subset_block_sizes, subset_block_holder_mapping,
            self._plasma_store_socket_name)

    def set_plasma_store_socket_name(self, plasma_store_socket_name: Optional[str]):
        self._plasma_store_socket_name = plasma_store_socket_name

    def clean(self, destroy: bool = False) -> NoReturn:
        if not self._resolved:
            return
        grouped = defaultdict(lambda: [])
        for i in self._resolved_block:
            label, index = self._fetch_indexes[i]
            grouped[label].append(index)

        for label in grouped:
            holder = self._block_holder_mapping[label]
            if holder:
                ray.get(holder.remove_object_ids.remote(grouped[label], destroy))

        self._fetch_indexes: List[Tuple[str, int]] = []
        self._resolved = False
        self._resolved_block = {}
        self._block_holder_mapping = None

    def __getitem__(self, item) -> pd.DataFrame:
        assert self._resolved, "You should resolve the SharedDataset before get item"
        object_id = self._resolved_block.get(item, None)
        assert object_id is not None, f"The {item} block has not been resolved"
        data = self._get_data_func(object_id)
        return pickle.loads(data)

    def __len__(self):
        """This return the block sizes in this block set"""
        return len(self._block_sizes)

    @classmethod
    def _custom_deserialize(cls,
                            fetch_indexes: List[Tuple[str, int]],
                            block_sizes: List[int],
                            block_holder_mapping: Dict[str, BlockHolderActorHandlerWrapper],
                            resolved_block: Dict[int, ray.ObjectID],
                            plasma_store_socket_name: str):
        instance = cls(fetch_indexes, block_sizes, block_holder_mapping, plasma_store_socket_name)
        instance.set_resolved_block(resolved_block)
        return instance

    def __reduce__(self):
        return (StandaloneClusterSharedDataset._custom_deserialize,
                (self._fetch_indexes, self._block_sizes, self._block_holder_mapping,
                 self._resolved_block, self._plasma_store_socket_name))

    def __del__(self):
        if ray.is_initialized():
            self.clean()
