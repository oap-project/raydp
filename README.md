# RayDP: Distributed Data processing on Ray

## Introduction
RayDP is a library can help you finish an end to end job for data processing and model training on single `python` file or with jupyter notebook. [Ray](https://github.com/ray-project/ray/) is an easy and powerful framework and provides useful tools for AI problems(such as, RLlib, Tune, RaySGD and more). And [Apache Spark](https://github.com/apache/spark) is a very popular distribute data processing and analytics framework. RayDP can be considered as the intersection of Apache Spark and Ray. It brings Apache Spark powerful data processing ability into Ray ecosystem. And it allows data processing and model training to coexist better.

![stack](doc/stack.png)

## Key Features

### Spark on Ray

We bring Apache Spark as a data processing framework on Ray and seamlessly integrated with other Ray libraries. We now support two ways to running Spark on Ray:

***Standalone***: We will startup a spark standalone cluster to running Spark.

***Native***: In this way, all the Spark executors are running in Ray java actors. And we startup a Ray java actor acts as Spark AppMaster for start/stop Spark executors. We could exchange data between Spark and Ray other components by Ray object store and leverage Apache Arrow format to decrease serialization/deserialization overhead.

### Estimator API for RaySGD

We provide a scikit-learn like API for RaySGD and supports training and evaluating on pyspark DataFrame directly. This hides the underlying details for distributed model training and data exchanging between Spark and Ray. 

## Build and Install

> **Note**: RayDP depends on the Ray and Apache Spark. However, we have to do some modification of the source code for those two framework due to the following reasons. **We will patch those modification to upstream later**. 
>
> * In Spark 3.0 and 3.0.1 version, pyspark does not support user defined resource manager.
> * In Ray 0.8.7 version, we can not esay exchange ray ObjectRef between different language workers.



You can build with the following command:

```shell
# build patched spark, based on spark 3.0
export RAYDP_BUILD_PYSPARK=1
# build patched ray, based on ray 0.8.7
export RAYDP_BUILD_RAY=1
${RAYDP_HOME}/.build.sh
```

You can find all the `whl` file under `${RAYDP_HOME}/dist`.

## Example

PLAsTiCC Astronomical Classification(https://www.kaggle.com/c/PLAsTiCC-2018)

![example](doc/example.png)

You can find the `NYC_Taxi` example under the `examples` folder.