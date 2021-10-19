# RayDP

RayDP is a distributed data processing library that provides simple APIs for running Spark on [Ray](https://github.com/ray-project/ray) and integrating Spark with distributed deep learning and machine learning frameworks. RayDP makes it simple to build distributed end-to-end data analytics and AI pipeline. Instead of using lots of glue code or an orchestration framework to stitch multiple distributed programs, RayDP allows you to write Spark, PyTorch, Tensorflow, XGBoost code in a single python program with increased productivity and performance. You can build an end-to-end pipeline on a single Ray cluster by using Spark for data preprocessing, RaySGD or Horovod for distributed deep learning, RayTune for hyperparameter tuning and RayServe for model serving.

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
spark = raydp.init_spark(app_name = 'RayDP Example',
                         num_executors = 2,
                         executor_cores = 2,
                         executor_memory = '4G')

# normal data processesing with Spark
df = spark.createDataFrame([('look',), ('spark',), ('tutorial',), ('spark',), ('look', ), ('python', )], ['word'])
df.show()
word_count = df.groupBy('word').count()
word_count.show()

# stop the spark cluster
raydp.stop_spark()
```

Spark features such as dynamic resource allocation, spark-submit script, etc are also supported. Please refer to [Spark on Ray](./doc/spark_on_ray.md) for more details.

## Machine Learning and Deep Learning With a Spark DataFrame

RayDP provides APIs for converting a Spark DataFrame to a Ray Dataset or Ray MLDataset which can be consumed by XGBoost, RaySGD or Horovod on Ray. RayDP also provides high level scikit-learn style Estimator APIs for distributed training with PyTorch or Tensorflow.


***Spark DataFrame <=> Ray Dataset***
```python
import ray
import raydp

ray.init()
spark = raydp.init_spark(app_name="RayDP example",
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

***Spark DataFrame => Ray MLDataset***

RayDP provides an API for creating a Ray MLDataset from a Spark dataframe. MLDataset can be converted to a PyTorch or Tensorflow dataset for distributed training with Horovod on Ray or RaySGD. MLDataset is also supported by XGBoost on Ray as a data source.

```python
import ray
import raydp
from raydp.spark import RayMLDataset

ray.init()
spark = raydp.init_spark(app_name="RayDP example",
                         num_executors=2,
                         executor_cores=2,
                         executor_memory="4GB")

df = spark.range(0, 1000)
ds = RayMLDataset.from_spark(df, num_shards=10)
```
Please refer to [Spark+Horovod on Ray](./examples/horovod_nyctaxi.py) for a full example.

***Estimator API***

The Estimator APIs allow you to train a deep neural network directly on a Spark DataFrame, leveraging Ray’s ability to scale out across the cluster. The Estimator APIs are wrappers of RaySGD and hide the complexity of converting a Spark DataFrame to a PyTorch/Tensorflow dataset and distributing the training. RayDP provides `raydp.torch.TorchEstimator` for PyTorch and `raydp.tf.TFEstimator` for Tensorflow. The following is an example of using TorchEstimator.

```python
import ray
import raydp
from raydp.torch import TorchEstimator

ray.init(address="auto")
spark = raydp.init_spark(app_name="RayDP example",
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

## MPI on Ray

RayDP also provides an API for running MPI job on Ray. Currently, we support three types of MPI: `intel_mpi`, `openmpi` and `MPICH`. You can refer [doc/mpi.md](./doc/mpi.md) for more details.

## More Examples
Not sure how to use RayDP? Check the `examples` folder. We have added many examples showing how RayDP works together with PyTorch, TensorFlow, XGBoost, Horovod, and so on. If you still cannot find what you want, feel free to post an issue to ask us!
