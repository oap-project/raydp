# RayDP

RayDP is a distributed data processing library that provides simple APIs for running Spark on [Ray](https://github.com/ray-project/ray) and integrating Spark with distributed deep learning and machine learning frameworks. RayDP makes it simple to build distributed end-to-end data analytics and AI pipeline. Instead of using lots of glue code or an orchestration framework to stitch multiple distributed programs, RayDP allows you to write Spark, PyTorch, Tensorflow, XGBoost code in a single python program with increased productivity and performance. You can build an end-to-end pipeline on a single Ray cluster by using Spark for data preprocessing, RaySGD or Horovod for distributed deep learning, RayTune for hyperparameter tuning and RayServe for model serving.

### Spark on Ray

RayDP provides an API for starting a Spark job on Ray in your python program without a need to setup a Spark cluster manually. RayDP supports Ray as a Spark resource manger and runs Spark executors in Ray actors. RayDP utilizes Ray's in-memory object store to efficiently exchange data between Spark and other Ray libraries. You can use Spark to read the input data, process the data using SQL, Spark DataFrame, or Pandas (via [Koalas](https://github.com/databricks/koalas)) API, extract and transform features using Spark MLLib, and feed the output to deep learning and machine learning frameworks.

### Integrating Spark with Deep Learning and Machine Learning Frameworks

#### MLDataset API
RayDP provides an API to create a Ray MLDataset from a Spark dataframe. MLDataset represents a distributed dataset stored in Ray's in-memory object store. It supports transformation on each shard and can be converted to a PyTorch or Tensorflow dataset for distributed training. If you prefer to using Horovod on Ray or RaySGD for distributed training, you can use MLDataset to seamlessly integrate Spark with them.

#### Estimator API
RayDP also provides high level scikit-learn style Estimator APIs for distributed training. The Estimator APIs allow you to train a deep neural network directly on a Spark DataFrame, leveraging Ray’s ability to scale out across the cluster. The Estimator APIs are wrappers of RaySGD and hide the complexity of converting a Spark DataFrame to a PyTorch/Tensorflow dataset and distributing the training.

## Installation


You can install latest RayDP using pip. RayDP requires Ray (>=1.1.0) and PySpark (>=3.0.0). Please also make sure java is installed and JAVA_HOME is set properly.
```shell
pip install raydp
```

Or you can install our nightly build:
```shell
pip install raydp-nightly
```

If you'd like to build and install the latest master, use the following command:

```shell
./build.sh
pip install dist/raydp*.whl
```

## Getting Started
To start a Spark job on Ray, you can use the `raydp.init_spark` API. You can write Spark, PyTorch/Tensorflow, Ray code in the same python program to easily implement an end-to-end pipeline.

### Classic Spark Word Count Example
After we use RayDP to initialize a Spark cluster, we can use Spark as usual. 
```python
import ray
import raydp

ray.init(address='auto')

spark = raydp.init_spark('word_count',
                         num_executors=2,
                         executor_cores=2,
                         executor_memory='1G')

df = spark.createDataFrame([('look',), ('spark',), ('tutorial',), ('spark',), ('look', ), ('python', )], ['word'])
df.show()
word_count = df.groupBy('word').count()
word_count.show()

raydp.stop_spark()
```

### Integration with PyTorch
However, combined with other ray components, such as RaySGD and RayServe, we can easily build an end-to-end deep learning pipeline. In this example. we show how to use our estimator API, which is a wrapper around RaySGD, to perform data preprocessing using Spark, and train a model using PyTorch.
```python
import ray
import raydp
from raydp.torch import TorchEstimator

ray.init()
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
## More Examples
Not sure how to use RayDP? Check the `examples` folder. We have added many examples showing how RayDP works together with PyTorch, TensorFlow, XGBoost, Horovod, and so on. If you still cannot find what you want, feel free to post an issue to ask us!
