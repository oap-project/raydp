import functools
import time
from collections import OrderedDict

import pandas as pd
import ray.util.data as ml_data
import raydp
import torch
from raydp.spark import RayMLDataset
from raydp.mpi import create_mpi_job, MPIJobContext

from dlrm.dlrm_s_pytorch import ModelArguments
from dlrm.spark_preproc import transform, generate_models, PreProcArguments

columns = [f"_c{i}" for i in range(40)]


def collate_fn(
        df: pd.DataFrame,
        num_numerical_features: int,
        max_ind_range: int,
        flag_input_torch_tensor=False):
    df = df[columns]
    # array = df.values
    # x_int_batch = array[:, 1:1 + num_numerical_features].astype(dtype=np.float32)
    # x_cat_batch = array[:, 1 + num_numerical_features:].astype(dtype=np.int64)
    # y_batch = array[:, 0].astype(dtype=np.float32)
    # x_int_batch = torch.from_numpy(x_int_batch)
    # x_cat_batch = torch.from_numpy(x_cat_batch)
    # y_batch = torch.from_numpy(y_batch)
    tensor = torch.from_numpy(df.values).view((-1, 40))
    x_int_batch = tensor[:, 1:1 + num_numerical_features]
    x_cat_batch = tensor[:, 1 + num_numerical_features:]
    y_batch = tensor[:, 0]

    if max_ind_range > 0:
        x_cat_batch = x_cat_batch % max_ind_range

    if flag_input_torch_tensor:
        x_int_batch = torch.log(x_int_batch.clone().detach().type(torch.float) + 1)
        x_cat_batch = x_cat_batch.clone().detach().type(torch.long)
        y_batch = y_batch.clone().detach().type(torch.float32).view(-1, 1)
    else:
        x_int_batch = torch.log(torch.tensor(x_int_batch, dtype=torch.float) + 1)
        x_cat_batch = torch.tensor(x_cat_batch, dtype=torch.long)
        y_batch = torch.tensor(y_batch, dtype=torch.float32).view(-1, 1)

    batch_size = x_cat_batch.shape[0]
    feature_count = x_cat_batch.shape[1]
    lS_o = torch.arange(batch_size).reshape(1, -1).repeat(feature_count, 1)

    return x_int_batch, lS_o, x_cat_batch.t(), y_batch.view(-1, 1)


def data_preprocess(
        app_name: str = None,
        num_executors: int = None,
        executor_cores: int = None,
        executor_memory: str = None,
        configs=None,
        args: PreProcArguments = None,
        return_df: bool = False):
    try:
        start = time.time()
        spark = raydp.init_spark(
            app_name=app_name,
            num_executors=num_executors,
            executor_cores=executor_cores,
            executor_memory=executor_memory,
            configs=configs)

        # generate models
        generate_models(spark, args, args.total_days_range)
        args.low_mem = False

        # transform for train data
        train_data, model_size = transform(spark, args, args.train_days_range, True, return_df)

        # transform for test data
        args.output_ordering = "input"
        test_data, _ = transform(spark, args, args.test_days_range, False, return_df)

        print("Data preprocessing duration:", time.time() - start)

        return train_data, test_data, model_size
    except:
        raise
    finally:
        if not return_df:
            raydp.stop_spark()


def create_train_task(
        num_workers: int = None,
        train_ds: ml_data.MLDataset = None,
        test_ds: ml_data.MLDataset = None,
        model_size=None,
        args: ModelArguments = None,
        nbatches: int = -1,
        nbatches_test: int = -1):

    fn = functools.partial(collate_fn,
                           num_numerical_features=args.arch_dense_feature_size,
                           max_ind_range=args.max_ind_range,
                           flag_input_torch_tensor=True)

    def task_fn(context):
        batch_size = args.mini_batch_size // num_workers
        train_loader = RayMLDataset.to_torch(ds=train_ds,
                                             world_size=num_workers,
                                             world_rank=context.world_rank,
                                             batch_size=batch_size,
                                             collate_fn=fn,
                                             shuffle=True,
                                             shuffle_seed=int(time.time()),
                                             local_rank=context.local_rank,
                                             prefer_node=context.node_ip,
                                             prefetch=True)

        test_batch_size = args.test_mini_batch_size // num_workers
        test_loader = RayMLDataset.to_torch(ds=test_ds,
                                            world_size=num_workers,
                                            world_rank=context.world_rank,
                                            batch_size=test_batch_size,
                                            collate_fn=fn,
                                            shuffle=True,
                                            shuffle_seed=int(time.time()),
                                            local_rank=context.local_rank,
                                            prefer_node=context.node_ip,
                                            prefetch=True)

        from dlrm.dlrm_s_pytorch import model_training
        try:
            model_training(args, train_loader, test_loader, model_size,
                           nbatches, nbatches_test)
        except Exception as e:
            time.sleep(1)
            print(e, flush=True)
        finally:
            return context.world_rank
    return task_fn


def main():
    shuffle_seed = int(time.time())
    pre_proc_args = PreProcArguments(total_days="0-23",
                                     train_days="0-22",
                                     test_days="23-23",
                                     input_folder="hdfs://sr133:9000/dlrm/input/",
                                     test_input_folder="hdfs://sr133:9000/dlrm/test/",
                                     output_folder="hdfs://sr133:9000/dlrm/output/",
                                     model_size_file=True,
                                     model_folder="hdfs://sr133:9000/dlrm/model/",
                                     write_mode="overwrite",
                                     dict_build_shuffle_parallel_per_day=2,
                                     low_mem=False,
                                     frequency_limit="15")

    extra_config = {"spark.local.dir": "/mnt/DP_disk1/xianyang/spark,/mnt/DP_disk2/xianyang/spark,/mnt/DP_disk3/xianyang/spark,/mnt/DP_disk4/xianyang/spark",
                    "spark.network.timeout": "1800s",
                    "spark.driver.memory": "100g",
                    "spark.sql.files.maxPartitionBytes": 1073741824,
                    "spark.driver.maxResultSize": "10G",
                    "spark.locality.wait": "1s",
                    "spark.sql.autoBroadcastJoinThreshold": "500M",
                    "spark.network.timeout": "1800s",
                    "spark.sql.broadcastTimeout": "1800s",
                    "spark.sql.adaptive.enabled": "false",
                    "spark.sql.shuffle.partitions": 600,
                    "spark.executor.extraJavaOptions": "-XX:+UseParallelGC -XX:+UseParallelOldGC -XX:+UseAdaptiveSizePolicy"}

    start = time.time()
    train_data, test_data, model_size = data_preprocess(app_name="dlrm",
                                                        num_executors=12,
                                                        executor_cores=12,
                                                        executor_memory="110GB",
                                                        configs=extra_config,
                                                        args=pre_proc_args,
                                                        return_df=False)
    model_size = OrderedDict([(key, value + 1) for key, value in model_size.items()])
    print(time.time() - start)

    def mpi_script_prepare_fn(context: MPIJobContext):
        extra_env = {}
        extra_env["http_proxy"] = ""
        extra_env["https_proxy"] = ""

        extra_env["KMP_BLOCKTIME"] = "1"
        extra_env["KMP_AFFINITY"] = "granularity=fine,compact,1,0"
        extra_env["LD_PRELOAD"] = "/home/xianyang/sw/miniconda3/envs/test/lib/libtcmalloc.so"
        extra_env["CCL_WORKER_COUNT"] = "4"
        extra_env["CCL_WORKER_AFFINITY"] = "0,1,2,3,24,25,26,27,"
        extra_env["CCL_ATL_TRANSPORT"] = "ofi"
        extra_env["OMP_NUM_THREADS"] = "20"

        scripts = []
        scripts.append("mpiexec.hydra")
        scripts.append("-genv")
        scripts.append("I_MPI_PIN_DOMAIN=[0xfffff0,0xfffff0000000,]")
        scripts.append("-hosts")
        scripts.append(",".join(context.hosts))
        scripts.append("-ppn")
        scripts.append(str(context.num_procs_per_node))
        scripts.append("-prepend-rank")
        return scripts

    job = create_mpi_job(job_name="dlrm",
                         world_size=4,
                         num_cpus_per_process=1,
                         num_processes_per_node=2,
                         mpi_script_prepare_fn=mpi_script_prepare_fn,
                         timeout=5,
                         mpi_type="intel_mpi")

    addresses = job.get_rank_addresses()

    def set_master_addr(context):
        import os
        os.environ["MASTER_ADDR"] = str(addresses[0])
        return context.world_rank

    job.run(set_master_addr)

    train_ds = RayMLDataset.from_parquet(paths=train_data,
                                         num_shards=4,
                                         shuffle=True,
                                         shuffle_seed=shuffle_seed,
                                         node_hints=addresses)
    test_ds = RayMLDataset.from_parquet(paths=test_data,
                                        num_shards=4,
                                        shuffle=True,
                                        shuffle_seed=shuffle_seed,
                                        node_hints=addresses)

    model_args = ModelArguments(dist_backend="ccl",
                                sparse_dense_boundary=2048,
                                bf16=True,
                                use_ipex=True,
                                arch_dense_feature_size=13,
                                arch_sparse_feature_size=128,
                                arch_mlp_bot="13-512-256-128",
                                arch_mlp_top="1024-1024-512-256-1",
                                max_ind_range=40000000,
                                loss_function="bce",
                                round_targets=True,
                                learning_rate=18.0,
                                mini_batch_size=32768,
                                test_mini_batch_size=65536,
                                print_freq=128,
                                print_time=True,
                                test_freq=6400,
                                lr_num_warmup_steps=8000,
                                lr_decay_start_step=70000,
                                lr_num_decay_steps=30000,
                                mlperf_logging=True,
                                mlperf_auc_threshold=0.8025,
                                mlperf_bin_shuffle=True,
                                numpy_rand_seed=12345,
                                data_generation="dataset",
                                data_set="terabyte",
                                memory_map=True,
                                processed_data_file="/mnt/DP_disk5/spark_preproc/dst/terabyte_processed.npz",
                                mlperf_bin_loader=True,
                                enable_profiling=False,
                                nepochs=1)

    train_fn = create_train_task(
        num_workers=4,
        train_ds=train_ds,
        test_ds=test_ds,
        model_size=model_size,
        args=model_args)
    job.run(train_fn)
    job.stop()
