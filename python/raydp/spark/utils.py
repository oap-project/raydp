import atexit
import psutil
import signal


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
    Random split the koalas.DataFrame into given part
    :param df: the koalas.DataFrame
    :param weights: list of doubles as weights with which to split the df.
                    Weights will be normalized if they don't sum up to 1.0.
    :param seed: The seed for sampling.
    """
    import databricks.koalas as ks
    # convert to Spark DataFrame
    sdf = df.to_spark()
    splits = sdf.randomSplit(weights, seed)
    # convert bach to koalas DataFrame
    return [ks.DataFrame(split) for split in splits]


