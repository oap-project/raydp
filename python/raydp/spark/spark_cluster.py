import copy
import numpy as np

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

import ray
import ray.cloudpickle as rpickle

import pandas as pd

from typing import Any, Dict, List, Optional

from raydp.cluster import Cluster
from raydp.ray_cluster_resources import ClusterResources
from raydp.spark.dataholder import DataHolder, DataHolderActorHandlerWrapper, ObjectIdList
from raydp.spark.utils import get_node_address
from raydp.spark.spark_master_service import SparkMasterService
from raydp.spark.spark_worker_service import SparkWorkerService


_global_broadcasted = {}
_global_data_holder: Dict[str, DataHolderActorHandlerWrapper] = {}


default_config = {
    "spark.sql.execution.arrow.enabled": "true"
}


class SparkCluster(Cluster):
    def __init__(self,
                 spark_home: str,
                 master_resources: Dict[str, float] = {"num_cpus": 0},
                 master_port: int = 8080,
                 master_webui_port: Any = None,
                 master_properties: Dict[str, str] = None):
        assert ray.is_initialized()
        self._ray_redis_address = ray.worker._global_node.redis_address
        self._ray_redis_password = ray.worker._global_node.redis_password

        self._spark_home = spark_home
        self._master_port = master_port
        self._master_webui_port = master_webui_port
        self._master_properties = master_properties

        self._master = None
        self._workers = []

        self._node_selected = set()

        self._stopped = False

        super().__init__(master_resources)

        self._set_up_master(master_resources, None)

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
        master_remote = ray.remote(**resources)(SparkMasterService)
        self._master = master_remote.remote(
            self._spark_home, self._master_port, self._master_webui_port, self._master_properties)
        # start up master
        error_msg = ray.get(self._master.start_up.remote())
        if not error_msg:
            # get master url after master start up
            self._master_url = ray.get(self._master.get_master_url.remote())
        else:
            del self._master
            self._master = None
            raise Exception(error_msg)

    def _set_up_worker(self, resources: Dict[str, float], kwargs: Dict[str, str]):
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
        worker_remote = ray.remote(**resources_copy)(SparkWorkerService)
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
            if choosed not in _global_data_holder:
                # set up data holder if has not set up
                data_holder = DataHolder.remote()
                _global_data_holder[choosed] = DataHolderActorHandlerWrapper(data_holder)

            self._workers.append(worker)
            self._num_nodes += 1
        else:
            del worker
            raise Exception(error_msg)

    def get_cluster_url(self) -> str:
        return self._master_url

    def get_spark_session(self,
                          app_name: str,
                          num_executors: int,
                          executor_cores: int,
                          executor_memory: str,
                          **kwargs) -> pyspark.sql.SparkSession:
        conf = default_config
        conf.update(kwargs)

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

    def stop(self):
        if not self._stopped:
            # destroy all data holder
            ray.get([holder.stop.remote() for holder in _global_data_holder.values()])
            _global_data_holder.clear()
            # TODO: wrap SparkSession to remove this
            _global_broadcasted.clear()
            # kill master
            if self._master:
                ray.get(self._master.stop.remote())

            if self._workers:
                ray.get([worker.stop.remote() for worker in self._workers])

            del self._master
            for worker in self._workers:
                del worker

            self._master_url = None
            self._stopped = True
            global _global_broadcasted_redis_address
            _global_broadcasted_redis_address = None


def save_to_ray(df: Any) -> ObjectIdList:
    if not isinstance(df, pyspark.sql.DataFrame):
        try:
            import databricks.koalas as ks
            if isinstance(df, ks.DataFrame):
                df = df.to_spark()
            else:
                raise Exception(f"The type: {type(df)} is not supported, only support "
                                "pyspark.sql.DataFram and databricks.koalas.DataFrame")
        except:
            raise Exception(f"The type: {type(df)} is not supported, only support "
                            "pyspark.sql.DataFram and databricks.koalas.DataFrame")


    return_type = StructType()
    return_type.add(StructField("node_label", StringType(), True))
    return_type.add(StructField("fetch_index", IntegerType(), True))

    @pandas_udf(return_type, PandasUDFType.MAP_ITER)
    def save(batch_iter):
        if not ray.is_initialized():
            redis_config = {}
            broadcased = _global_broadcasted["redis"].value
            redis_config["address"] = broadcased["address"]
            redis_config["password"] = broadcased["password"]

            local_address = get_node_address()

            ray.init(address=redis_config["address"],
                     node_ip_address=local_address,
                     redis_password=redis_config["password"])

        node_label = f"node:{ray.services.get_node_ip_address()}"
        data_handler = _global_data_holder[node_label]
        for pdf in batch_iter:
            obj = ray.put(pdf)
            # TODO: register object in batch
            fetch_index = ray.get(data_handler.register_object_id.remote(rpickle.dumps(obj)))
            yield pd.DataFrame({"node_label": [node_label], "fetch_index": [fetch_index]})

    results = df.mapInPandas(save).collect()
    fetch_indexes = [(row["node_label"], row["fetch_index"]) for row in results]
    return ObjectIdList(fetch_indexes)
