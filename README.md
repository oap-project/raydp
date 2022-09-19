# RayDP

RayDP is a distributed data processing library that provides simple APIs for running Spark on [Ray](https://github.com/ray-project/ray) and integrating Spark with distributed deep learning and machine learning frameworks. RayDP makes it simple to build distributed end-to-end data analytics and AI pipeline. Instead of using lots of glue code or an orchestration framework to stitch multiple distributed programs, RayDP allows you to write Spark, PyTorch, Tensorflow, XGBoost code in a single python program with increased productivity and performance. You can build an end-to-end pipeline on a single Ray cluster by using Spark for data preprocessing, Ray Train or Horovod for distributed deep learning, RayTune for hyperparameter tuning and RayServe for model serving.

## Installation


You can install latest RayDP using pip. RayDP requires Ray and PySpark. Please also make sure java is installed and JAVA_HOME is set properly.

```shell
pip install raydp
```

Or you can install RayDP nightly build:

```shell
pip install raydp-nightly
```

If you'd like to build and install the latest master, use the following command:

```shell
./build.sh
pip install dist/raydp*.whl
```

## Spark on Ray

RayDP provides an API for starting a Spark job on Ray without a need to setup a Spark cluster separately. RayDP supports Ray as a Spark resource manager and runs Spark executors in Ray actors. To create a Spark session, call the `raydp.init_spark` API. For example:

```python
import ray
import raydp

# connect to ray cluster
ray.init(address='auto')

# create a Spark cluster with specified resource requirements
spark = raydp.init_spark(app_name='RayDP Example',
                         num_executors=2,
                         executor_cores=2,
                         executor_memory='4GB')

# normal data processesing with Spark
df = spark.createDataFrame([('look',), ('spark',), ('tutorial',), ('spark',), ('look', ), ('python', )], ['word'])
df.show()
word_count = df.groupBy('word').count()
word_count.show()

# stop the spark cluster
raydp.stop_spark()
```

Spark features such as dynamic resource allocation, spark-submit script, etc are also supported. Please refer to [Spark on Ray](./doc/spark_on_ray.md) for more details.

### Ray Client

RayDP works the same way when using ray client. However, spark driver would be on the local machine. This is convenient if you want to do some experiment in an interactive environment. If this is not desired, e.g. due to performance, you can define an ray actor, which calls `init_spark` and performs all the calculation in its method. This way, spark driver will be in the ray cluster, and is rather similar to spark cluster deploy mode.

## Pandas on Spark

PySpark 3.2.0 provides Pandas API on Spark(originally Koalas). Users familiar with Pandas can use it to scale current pandas workloads on RayDP.

```python
import ray
import raydp
import pyspark.pandas as ps

# connect to ray cluster
ray.init(address='auto')

# create a Spark cluster with specified resource requirements
spark = raydp.init_spark(app_name='RayDP Example',
                         num_executors=2,
                         executor_cores=2,
                         executor_memory='4GB')

# Use pandas on spark to create a dataframe and aggregate
psdf = ps.range(100)
print(psdf.count())

# stop the spark cluster
raydp.stop_spark()
```

## Machine Learning and Deep Learning With a Spark DataFrame

RayDP provides APIs for converting a Spark DataFrame to a Ray Dataset which can be consumed by XGBoost, Ray Train or Horovod on Ray. RayDP also provides high level scikit-learn style Estimator APIs for distributed training with PyTorch or Tensorflow.


***Spark DataFrame <=> Ray Dataset***
```python
import ray
import raydp

ray.init()
spark = raydp.init_spark(app_name="RayDP Example",
                         num_executors=2,
                         executor_cores=2,
                         executor_memory="4GB")

# Spark Dataframe to Ray Dataset
df1 = spark.range(0, 1000)
ds1 = ray.data.from_spark(df1)

# Ray Dataset to Spark Dataframe
ds2 = ray.data.from_items([{"id": i} for i in range(1000)])
df2 = ds2.to_spark(spark)
```
Please refer to [Spark+XGBoost on Ray](./examples/xgboost_ray_nyctaxi.py) for a full example.

***Estimator API***

The Estimator APIs allow you to train a deep neural network directly on a Spark DataFrame, leveraging Ray’s ability to scale out across the cluster. The Estimator APIs are wrappers of Ray Train and hide the complexity of converting a Spark DataFrame to a PyTorch/Tensorflow dataset and distributing the training. RayDP provides `raydp.torch.TorchEstimator` for PyTorch and `raydp.tf.TFEstimator` for Tensorflow. The following is an example of using TorchEstimator.

```python
import ray
import raydp
from raydp.torch import TorchEstimator

ray.init(address="auto")
spark = raydp.init_spark(app_name="RayDP Example",
                         num_executors=2,
                         executor_cores=2,
                         executor_memory="4GB")
                         
# Spark DataFrame Code 
df = spark.read.parquet(…) 
train_df = df.withColumn(…)

# PyTorch Code 
model = torch.nn.Sequential(torch.nn.Linear(2, 1)) 
optimizer = torch.optim.Adam(model.parameters())

estimator = TorchEstimator(model=model, optimizer=optimizer, ...) 
estimator.fit_on_spark(train_df)

raydp.stop_spark()
```
Please refer to [NYC Taxi PyTorch Estimator](./examples/pytorch_nyctaxi.py) and [NYC Taxi Tensorflow Estimator](./examples/tensorflow_nyctaxi.py) for full examples.

***Fault Tolerance***

The ray dataset converted from spark dataframe like above is not fault-tolerant. This is because we implement it using `Ray.put` combined with spark `mapPartitions`. Objects created by `Ray.put` is not recoverable in Ray.

RayDP now supports converting data in a way such that the resulting ray dataset is fault-tolerant. This feature is currently *experimental*. Here is how to use it:
```python
import ray
import raydp

ray.init(address="auto")
# set fault_tolerance_mode to True to enable the feature
# this will connect pyspark driver to ray cluster
spark = raydp.init_spark(app_name="RayDP Example",
                         num_executors=2,
                         executor_cores=2,
                         executor_memory="4GB",
                         fault_tolerance_mode=True)
# df should be large enough so that result will be put into plasma
df = spark.range(100000)
# use this API instead of ray.data.from_spark
ds = raydp.spark.from_spark_recoverable(df)
# ds is now fault-tolerant.
```
Notice that `from_spark_recoverable` will persist the converted dataframe. You can provide the storage level through keyword parameter `storage_level`. In addition, this feature is not available in ray client mode. If you need to use ray client, please wrap your application in a ray actor, as described in the ray client chapter.

## MPI on Ray

RayDP also provides an API for running MPI job on Ray. We support three types of MPI: `intel_mpi`, `openmpi` and `MPICH`. You can refer to [doc/mpi.md](./doc/mpi.md) for more details.

## More Examples
### Examples
Not sure how to use RayDP? Check the `examples` folder. We have added many examples showing how RayDP works together with PyTorch, TensorFlow, XGBoost, Horovod, and so on. If you still cannot find what you want, feel free to post an issue to ask us!
### Google Colab Notebook
You can easily run code on Google Colab with a google account. Maybe this is the easiest way to get started with raydp, you can have a try. Here are two demos: [Raytrain example](https://colab.research.google.com/github/oap-project/raydp/blob/master/tutorials/raytrain_example.ipynb), [Pytorch example](https://colab.research.google.com/github/oap-project/raydp/blob/master/tutorials/pytorch_example.ipynb).

