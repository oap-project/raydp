
### External Shuffle Service & Dynamic Resource Allocation

RayDP supports External Shuffle Serivce. To enable it, you can either set `spark.shuffle.service.enabled` to `true` in `spark-defaults.conf`, or you can provide a config to `raydp.init_spark`, as shown below:

```python
raydp.init_spark(..., configs={"spark.shuffle.service.enabled": "true"})
```

The user-provided config will overwrite those specified in `spark-defaults.conf`. By default Spark will load `spark-defaults.conf` from `$SPARK_HOME/conf`, you can also modify this location by setting `SPARK_CONF_DIR`.

Similarly, you can also enable Dynamic Executor Allocation this way. However, currently you must use Dynamic Executor Allocation with data persistence. You can write the data frame in spark to HDFS as a parquet as shown below:

```python
ds = RayMLDataset.from_spark(..., fs_directory="hdfs://host:port/your/directory")
```

### RayDP Executor Extra ClassPath 

As raydp starts the java executors, the classpath will contain the absolute path of ray, raydp and spark by default. When you run ray cluster on yarn([Deploying on Yarn](https://docs.ray.io/en/latest/cluster/yarn.html)), the jar files are stored on HDFS, which have different absolute path in each node. In such cases,  jvm cannot find the main class and ray workers will fail to start. 

To solve such problems, users can specify extra classpath when` init _spark` by configuring `raydp.executor.extraClassPath`. Make sure your jar files are distributed to the same path(s) on all nodes of the ray cluster.

```python
raydp.init_spark(..., configs={"raydp.executor.extraClassPath": "/your/extra/jar/path:/another/path"})
```

### Spark Submit

RayDP provides a substitute for spark-submit in Apache Spark. You can run your java or scala application on RayDP cluster by using `bin/raydp-submit`. You can add it to `PATH` for convenience. When using `raydp-submit`, you should specify number of executors, number of cores and memory each executor by Spark properties, such as `--conf spark.executor.cores=1`, `--conf spark.executor.instances=1` and `--conf spark.executor.memory=500m`. `raydp-submit` only supports Ray cluster. Spark standalone, Apache Mesos, Apache Yarn are not supported, please use traditional `spark-submit` in that case. For the same reason, you do not need to specify `--master` in the command. Besides, RayDP does not support cluster as deploy-mode.

### Placement Group
RayDP can leverage Ray's placement group feature and schedule executors onto spcecified placement group. It provides better control over the allocation of Spark executors on a Ray cluster, for example spreading the spark executors onto seperate nodes or starting all executors on a single node. You can specify a created placement group when init spark, as shown below:

```python
raydp.init_spark(..., placement_group=pg)
```

Or you can just specify the placement group strategy. RayDP will create a coreesponding placement group and manage its lifecycle, which means the placement group will be created together with SparkSession and removed when calling `raydp.stop_spark()`. Strategy can be "PACK", "SPREAD", "STRICT_PACK" or "STRICT_SPREAD". Please refer to [Placement Groups document](https://docs.ray.io/en/latest/placement-group.html#pgroup-strategy) for details.

```python
raydp.init_spark(..., placement_group_strategy="SPREAD")
```


### RayDP Hive Support
RayDP can read or write Hive, which might be useful if the data is stored in HDFS.If you want to enable this feature, please configure your environment as following:
+ Install spark to ray cluster's each node and set ENV SPARK_HOME
+ COPY your hdfs-site.xml and hive-site.xml to $SPARK_HOME/conf. If using hostname in your xml file, make sure /etc/hosts is set properly
+ Test: You can test if Hive configuration is successful like this
```python
from pyspark.sql.session import SparkSession
spark = SparkSession.builder().enableHiveSupport()
spark.sql("select * from db.xxx").show()  # db is database, xxx is exists table
```
RayDP using Hive example
```python
ray.init("auto")
spark = raydp.init_spark(...,enable_hive=True)
spark.sql("select * from db.xxx").show()
```


### Autoscaling
You can use RayDP with Ray autoscaling. When you call `raydp.init_spark`, the autoscaler will try to increase the number of worker nodes if the current capacity of the cluster can't meet the resource demands. However currently there is a known issue in Ray autoscaling. The autoscaler's default strategy is to avoid launching GPU nodes if there aren't any GPU tasks at all. So if you configure a single worker node type with GPU, by default the autosaler will not launch nodes to start Spark executors on them. To resolve the issue, you can either set the environment variable "AUTOSCALER_CONSERVE_GPU_NODES" to 0 or configure multiple node types that at least one is CPU only node. 


### Logging
+ Driver Log: By default, the spark driver log level is WARN. After getting a Spark session by running `spark = raydp.init_spark`, you can change the log level for example `spark.sparkContext.setLogLevel("INFO")`. You will also see some AppMaster INFO logs on the driver. This is because Ray redirects the actor logs to driver by default. To disable logging to driver, you can set it in Ray init `ray.init(log_to_driver=False)`
+ Executor Log: The spark executor logs are stored in Ray's logging directory. By default they are available at /tmp/ray/session_\*/logs/java-worker-\*.log
