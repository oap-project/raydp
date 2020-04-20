from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import BinaryType
from typing import List
from spark_on_ray.spark.spark_cluster import _global_broadcasted

import pandas as pd
import psutil
import ray
import ray.services
import ray.cloudpickle as rpickle


@pandas_udf(BinaryType())
def save_to_ray(*columns) -> List[bytes]:
    global redis_config
    if not redis_config:
        redis_config = {}
        broadcased = _global_broadcasted.value
        redis_config["address"] = broadcased["address"]
        redis_config["password"] = broadcased["password"]

    global local_address
    if not local_address:
        local_address = get_node_address()

    if not ray.is_initialized():
        ray.init(address=redis_config["address"],
                 node_ip_address=local_address,
                 redis_password=redis_config["password"])

    df = pd.concat(columns, axis=1, copy=False)
    id = ray.put(df)

    id_bytes = rpickle.dumps(id)
    return id_bytes


def load_into_ids(id_in_bytes: List[bytes]) -> List[ray.ObjectID]:
    return [rpickle.loads(i) for i in id_in_bytes]


def get_node_address() -> str:
    """
    Get the ip address used in ray.
    """
    pids = psutil.pids()
    for pid in pids:
        try:
            proc = psutil.Process(pid)
            # HACK: Workaround for UNIX idiosyncrasy
            # Normally, cmdline() is supposed to return the argument list.
            # But it in some cases (such as when setproctitle is called),
            # an arbitrary string resembling a command-line is stored in
            # the first argument.
            # Explanation: https://unix.stackexchange.com/a/432681
            # More info: https://github.com/giampaolo/psutil/issues/1179
            for arglist in proc.cmdline():
                for arg in arglist.split(" "):
                    if arg.startswith("--node-ip-address"):
                        addr = arg.split("=")[1]
                        return addr
        except psutil.AccessDenied:
            pass
        except psutil.NoSuchProcess:
            pass
    raise Exception("can't find any ray process")
