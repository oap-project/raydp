# RayDP

RayDP brings popular big data frameworks including [Apache Spark](https://github.com/apache/spark) to [Ray](https://github.com/ray-project/ray/) ecosystem and integrates with other Ray libraries seamlessly. RayDP makes it simple to build distributed end-to-end data analytics and AI pipeline on Ray by using Spark for data preprocessing, RayTune for hyperparameter tunning, RaySGD for distributed deep learning, RLlib for reinforcement learning and RayServe for model serving.

![stack](https://github.com/oap-project/raydp/blob/master/doc/stack.png)

## Key Features

### Spark on Ray

RayDP enables you to start a Spark job on Ray in your python program without a need to setup a Spark cluster manually. RayDP supports Ray as a Spark resource manger and starts Spark executors using Ray actor directly. RayDP utilizes Ray's in-memory object store to efficiently exchange data between Spark and other Ray libraries. You can use Spark to read the input data, process the data using SQL, Spark DataFrame, or Pandas (via [Koalas](https://github.com/databricks/koalas)) API, extract and transform features using Spark MLLib, and use RayDP Estimator API for distributed training on the preprocessed dataset. 

### Estimator APIs for Distributed Training

RayDP provides high level scikit-learn style Estimator APIs for distributed training. The Estimator APIs allow you to train a deep neural network directly on a Spark DataFrame, leveraging Ray’s ability to scale out across the cluster. The Estimator APIs are wrappers of RaySGD and hide the complexity of converting a Spark DataFrame to a PyTorch/Tensorflow dataset and distributing the training.

## Installation


You can install latest RayDP using pip. RayDP requires Ray (>=1.1.0) and PySpark (3.0.0 or 3.0.1).
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
To start a Spark job on Ray, you can use the `raydp.init_spark` API. You can write Spark, PyTorch/Tensorflow, Ray code in the same python program to easily implement an end to end pipeline.

### Classic Spark Word Count Example
After we use RayDP to initialize a Spark cluster, of course we can use Spark as usual. 
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
However, combined with other ray components, such as raysgd and ray serve, we can easily build an end-to-end deep learning pipeline. In this example. we show how to use our estimator API, which is a wrapper around raysgd, to perform data preprocessing using Spark, and train a model using PyTorch.
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
Not sure how to use RayDP? Check the `examples` folder. We have added many examples showing how RayDP works together with PyTorch, TensorFlow, XGBoost, Horovod, and so on. If you still cannot find what you want, feel free to post a issue to ask us!
