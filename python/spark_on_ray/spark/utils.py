import atexit
import psutil
import pandas as pd
import signal
from spark_on_ray.spark.dataholder import DataHolderActorHandlerWrapper, ObjectIdList
import tensorflow as tf
from typing import Dict, List


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


def register_exit_handler(func):
    atexit.register(func)
    signal.signal(signal.SIGTERM, func)
    signal.signal(signal.SIGINT, func)


def create_dataset_from_objects(
        objs: ObjectIdList,
        features_columns: List[str],
        label_column: str,
        data_holder_mapping: Dict[str, DataHolderActorHandlerWrapper]) -> tf.data.Dataset:
    # TODO: this will load all data into memory which is not optimized.
    objs.resolve(data_holder_mapping, True)
    # transfer to Dataset
    datasets: List[tf.data.Dataset] = \
        [tf.data.Dataset.from_tensor_slices((pdf[features_columns].values, pdf[label_column].values))
         for pdf in objs]
    assert len(datasets) > 0
    # concat
    result = datasets[0]
    for i in range(1, len(datasets)):
        result.concatenate(datasets[i])

    return result
