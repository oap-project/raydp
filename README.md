# RayDP

RayDP is a distributed data processing library that provides simple APIs for running Spark/MPI on [Ray](https://github.com/ray-project/ray) and integrating Spark with distributed deep learning and machine learning frameworks. RayDP makes it simple to build distributed end-to-end data analytics and AI pipeline. Instead of using lots of glue code or an orchestration framework to stitch multiple distributed programs, RayDP allows you to write Spark, PyTorch, Tensorflow, XGBoost code in a single python program with increased productivity and performance. You can build an end-to-end pipeline on a single Ray cluster by using Spark for data preprocessing, RaySGD or Horovod for distributed deep learning, RayTune for hyperparameter tuning and RayServe for model serving.

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

## Machine Learning and Deep Learning With a Spark DataFrame

RayDP provides APIs for converting Spark DataFrame to Ray Dataset or Ray MLDataset which can be used to connect to XGBoost, RaySGD or Horovod easily. RayDP also provides high level scikit-learn style Estimator APIs for distributed training with PyTorch or Tensorflow.


***Spark DataFrame <=> Ray Dataset***



***MLDataset API***

RayDP provides an API for creating a Ray MLDataset from a Spark dataframe. MLDataset represents a distributed dataset stored in Ray's in-memory object store. It supports transformation on each shard and can be converted to a PyTorch or Tensorflow dataset for distributed training. If you prefer to using Horovod on Ray or RaySGD for distributed training, you can use MLDataset to seamlessly integrate Spark with them.

***Estimator API***

RayDP also provides high level scikit-learn style Estimator APIs for distributed training. The Estimator APIs allow you to train a deep neural network directly on a Spark DataFrame, leveraging Ray’s ability to scale out across the cluster. The Estimator APIs are wrappers of RaySGD and hide the complexity of converting a Spark DataFrame to a PyTorch/Tensorflow dataset and distributing the training.

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

# You can use the RayDP Estimator API or libraries like RaySGD for distributed training.
estimator = TorchEstimator(model=model, optimizer=optimizer, ...) 
estimator.fit_on_spark(train_df)

raydp.stop_spark()
```

## MPI on Ray

RayDP also provides a simple API to running MPI job on top of Ray. Currently, we support three types of MPI: `intel_mpi`, `openmpi` and `MPICH`. You can refer [doc/mpi.md](./doc/mpi.md) for more details.

## More Examples
Not sure how to use RayDP? Check the `examples` folder. We have added many examples showing how RayDP works together with PyTorch, TensorFlow, XGBoost, Horovod, and so on. If you still cannot find what you want, feel free to post an issue to ask us!
