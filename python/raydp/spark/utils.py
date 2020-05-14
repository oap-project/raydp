import signal

import atexit
import psutil


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
        import databricks.koalas as ks
        return [ks.DataFrame(split) for split in splits]


def _df_helper(df, spark_callback, koalas_callback):
    try:
        import pyspark
    except Exception:
        pass
    else:
        if isinstance(df, pyspark.sql.DataFrame):
            return spark_callback(df)

    try:
        import databricks.koalas as ks
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
