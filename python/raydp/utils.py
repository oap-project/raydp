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

import atexit
import re
import signal
from typing import Dict, List

import psutil

MEMORY_SIZE_UNITS = {"K": 2**10, "M": 2**20, "G": 2**30, "T": 2**40}

# we use 4 bytes for block size, this means each block can contain
# 4294967296 records
BLOCK_SIZE_BIT = 32


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


def random_split(df, weights, seed=None):
    """
    Random split the spark DataFrame or koalas DataFrame into given part
    :param df: the spark DataFrame or koalas DataFrame
    :param weights: list of doubles as weights with which to split the df.
                    Weights will be normalized if they don't sum up to 1.0.
    :param seed: The seed for sampling.
    """
    # convert to Spark DataFrame
    df, is_spark_df = convert_to_spark(df)
    splits = df.randomSplit(weights, seed)
    if is_spark_df:
        return splits
    else:
        # convert back to koalas DataFrame
        import databricks.koalas as ks  # pylint: disable=C0415
        return [ks.DataFrame(split) for split in splits]


def _df_helper(df, spark_callback, koalas_callback):
    try:
        import pyspark  # pylint: disable=C0415
    except Exception:
        pass
    else:
        if isinstance(df, pyspark.sql.DataFrame):
            return spark_callback(df)

    try:
        import databricks.koalas as ks  # pylint: disable=C0415
    except Exception:
        pass
    else:
        if isinstance(df, ks.DataFrame):
            return koalas_callback(df)

    raise Exception(f"The type: {type(df)} is not supported, only support "
                    "pyspark.sql.DataFrame and databricks.koalas.DataFrame")


def df_type_check(df):
    """
    Check whether the df is spark DataFrame or koalas DataFrame.
    :return True for spark DataFrame or Koalas DataFrame.
    :raise Exception when it is neither spark DataFrame nor Koalas DataFrame.
    """
    return _df_helper(df, lambda d: True, lambda d: True)


def convert_to_spark(df):
    """
    Do nothing if the df is spark DataFrame, convert to spark DataFrame if it is
    koalas DataFrame. Raise Exception otherwise.
    :return: a pair of (converted df, whether it is spark DataFrame)
    """
    return _df_helper(df, lambda d: (d, True), lambda d: (d.to_spark(), False))


def parse_memory_size(memory_size: str) -> int:
    """
    Parse the human readable memory size into bytes.
    Adapt from: https://stackoverflow.com/a/60708339
    :param memory_size: human readable memory size
    :return: convert to int size
    """
    memory_size = memory_size.strip().upper()
    if re.search(r"B", memory_size):
        # discard "B"
        memory_size = re.sub(r"B", "", memory_size)

    try:
        return int(memory_size)
    except ValueError:
        pass

    global MEMORY_SIZE_UNITS
    if not re.search(r" ", memory_size):
        memory_size = re.sub(r"([KMGT]+)", r" \1", memory_size)
    number, unit_index = [item.strip() for item in memory_size.split()]
    return int(float(number) * MEMORY_SIZE_UNITS[unit_index])


def divide_blocks(
        blocks: List[int],
        world_size: int) -> Dict[int, List[int]]:
    """
    Divide the blocks into world_size partitions, and return the divided block indexes for the
    given work_rank
    :param blocks: the blocks and each item is the given block size
    :param world_size: total world size
    :return: a dict, the key is the world rank, and the value the block indexes
    """
    if len(blocks) < world_size:
        raise Exception("do not have enough blocks to divide")
    results = {}
    tmp_queue = {}
    for i in range(world_size):
        results[i] = []
        tmp_queue[i] = 0
    indexes = range(len(blocks))
    blocks_with_indexes = dict(zip(indexes, blocks))
    blocks_with_indexes = dict(sorted(blocks_with_indexes.items(),
                                      key=lambda item: item[1],
                                      reverse=True))
    for i, block in blocks_with_indexes.items():
        rank = sorted(tmp_queue, key=lambda x: tmp_queue[x])[0]
        results[rank].append(i)
        tmp_queue[rank] = tmp_queue[rank] + block

    for i, indexes in results.items():
        results[i] = sorted(indexes)
    return results
