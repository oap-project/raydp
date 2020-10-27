# RayDP

RayDP brings popular big data frameworks including [Apache Spark](https://github.com/apache/spark) to [Ray](https://github.com/ray-project/ray/) ecosystem and integrates with other Ray libraries seamlessly. RayDP makes it simple to build distributed end-to-end data analytics and AI pipeline on Ray by using Spark for data preprocessing, RayTune for hyperparameter tunning, RaySGD for distributed deep learning, RLlib for reinforcement learning and RayServe for model serving.

![stack](doc/stack.png)

## Key Features

### Spark on Ray

RayDP enables you to start a Spark job on Ray in your python program without a need to setup a Spark cluster manually. RayDP supports Ray as a Spark resource manger and starts Spark executors using Ray actor directly. RayDP utilizes Ray's in-memory object store to efficiently exchange data between Spark and other Ray libraries. You can use Spark to read the input data, process the data using SQL, Spark DataFrame, or Pandas (via [Koalas](https://github.com/databricks/koalas)) API, extract and transform features using Spark MLLib, and use RayDP Estimator API for distributed training on the preprocessed dataset. 

### Estimator APIs for Distributed Training

RayDP provides high level scikit-learn style Estimator APIs for distributed training. The Estimator APIs allow you to train a deep neural network directly on a Spark DataFrame, leveraging Ray’s ability to scale out across the cluster. The Estimator APIs are wrappers of RaySGD and hide the complexity of converting a Spark DataFrame to a PyTorch/Tensorflow dataset and distributing the training.

## Build and Install

> **Note**: RayDP depends on Ray and Apache Spark. However, we have to do some modification of the source code for those two frameworks due to the following reasons. **We will patch those modification to upstream later**. 
>
> * In Spark 3.0 and 3.0.1 version, pyspark does not support user defined resource manager.
> * In Ray 0.8.7 version, we can not easily exchange ray ObjectRef between different language workers.



You can build with the following command:

```shell
# build patched spark, based on spark 3.0
export RAYDP_BUILD_PYSPARK=1
# build patched ray, based on ray 0.8.7
export RAYDP_BUILD_RAY=1
${RAYDP_HOME}/.build.sh
```

You can find all the `whl` file under `${RAYDP_HOME}/dist`.

## Get Started

Write Spark, PyTorch/Tensorflow, Ray code in the same python program using RayDP.
```python
import ray
import raydp
from raydp.torch import TorchEstimator

ray.init(…) 
spark = raydp.init_spark(…)

# Spark DataFrame Code 
df = spark.read.parquet(…) 
train_df = df.withColumn(…)

# PyTorch Code 
model = torch.nn.Sequential(torch.nn.Linear(2, 1)) 
optimizer = torch.optim.Adam(model.parameters())

# Sklearn style Estimator API in RayDP for distributed training 
estimator = TorchEstimator(model=model, optimizer=optimizer, ...) 
estimator.fit_on_spark(train_df)

```

You can find more examples under the `examples` folder.
