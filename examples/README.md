# RayDP Examples
Here are a few examples showing how RayDP works together with other libraries, such as PyTorch, Tensorflow, XGBoost and Horovod. 

In order to run these examples, you may need to install corresponding dependencies. For installation guides, please refer to their homepages. Notice that we need to install [xgboost_ray](https://github.com/ray-project/xgboost_ray) to run the xgboost example. In addition, if you are running the examples in a ray cluster, all nodes should have the dependecies installed.

## NYC Taxi Fare Prediction Dataset
We have a few examples which use this dataset. The dataset can be downloaded [here](https://www.kaggle.com/c/new-york-city-taxi-fare-prediction/data). After you download it, please modify the variable `NYC_TRAIN_CSV` in `data_process.py` and point it to where `train.csv` is saved. 

## Horovod
When running `horovod_nyctaxi.py`, do not use `horovodrun`. Check [here](https://horovod.readthedocs.io/en/stable/ray_include.html) for more information.

## RaySGD Example
In the RaySGD example, we demonstrate how to use our `MLDataset` API. After we use Spark to transform the dataset, we call `create_ml_dataset_from_spark` to write the Spark DataFrames into Ray object store, using Apache Arrow format. We then convert the data to `pandas` DataFrame, hopefully zero-copy. Finally, they can be consumed by any framework supports `numpy` format, such as PyTorch or Tensorflow. `MLDataset` is partitioned, or sharded, just like Spark DataFrames. Their numbers of partitions are not required to be the same. However, the number of shards of `MLDataset` should be the same as the number of workers of `TorchTrainer` or `TFTrainer`, so that each worker is mapped to a shard.