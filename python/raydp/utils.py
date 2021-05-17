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
import math
import re
import signal
from typing import Dict, List, Tuple

import numpy as np
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
        world_size: int,
        shuffle: bool = False,
        shuffle_seed: int = None) -> Dict[int, List[int]]:
    """
    Divide the blocks into world_size partitions, and return the divided block indexes for the
    given work_rank
    :param blocks: the blocks and each item is the given block size
    :param world_size: total world size
    :param shuffle: whether shuffle the blocks before divide
    :param shuffle_seed: the shuffle seed
    :return: a dict, the key is the world rank, and the value is a list of pair of block index
             and the samples selected in that block
    """
    if len(blocks) < world_size:
        raise Exception("do not have enough blocks to divide")

    results = {}

    # number of blocks per rank
    num_blocks_per_rank = int(math.ceil(len(blocks) * 1.0 / world_size))
    # number of samples per rank
    num_samples_per_rank = int(math.ceil(sum(blocks) * 1.0 / world_size))
    # total number of blocks
    total_num_blocks = num_blocks_per_rank * world_size
    # global block indexes
    global_indexes = list(range(len(blocks)))

    # add extra blocks to make it evenly divisible
    if len(global_indexes) != total_num_blocks:
        global_indexes += global_indexes[: (total_num_blocks - len(global_indexes))]

    assert len(global_indexes) == total_num_blocks

    if shuffle_seed:
        np.random.seed(shuffle_seed)
    else:
        np.random.seed(0)

    if shuffle:
        np.random.shuffle(global_indexes)

    def select(index: int, current_size: int, selected: List[Tuple[int, int]]) -> int:
        block_size = blocks[index]
        tmp = current_size + block_size
        if tmp < num_samples_per_rank:
            selected.append((index, block_size))
            current_size = tmp
        elif tmp >= num_samples_per_rank:
            selected.append((index, (num_samples_per_rank - current_size)))
            current_size = num_samples_per_rank
        return current_size

    for rank in range(world_size):
        indexes = global_indexes[rank: total_num_blocks: world_size]
        assert len(indexes) == num_blocks_per_rank

        samples_cur_rank = 0
        selected_indexes = []
        for i in indexes:
            samples_cur_rank = select(i, samples_cur_rank, selected_indexes)
            if samples_cur_rank == num_samples_per_rank:
                break

        while samples_cur_rank < num_samples_per_rank:
            index = np.random.choice(global_indexes, size=1)[0]
            samples_cur_rank = select(index, samples_cur_rank, selected_indexes)

        assert samples_cur_rank == num_samples_per_rank

        results[rank] = selected_indexes

    return results
