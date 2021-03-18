# RayDP Examples
Here are a few examples showing how RayDP works together with other libraries, such as PyTorch, Tensorflow, XGBoost and Horovod. 

In order to run these examples, you may need to install corresponding dependencies. For installation guides, please refer to their homepages. Notice that we need to install [xgboost_ray](https://github.com/ray-project/xgboost_ray) to run the xgboost example. In addition, if you are running the examples in a ray cluster, all nodes should have the dependecies installed.

## NYC Taxi Fare Prediction Dataset
We have a few examples which use this dataset. The dataset can be downloaded [here](https://www.kaggle.com/c/new-york-city-taxi-fare-prediction/data). After you download it, please modify the variable `NYC_TRAIN_CSV` in `data_process.py` and point it to where `train.csv` is saved. 

## Horovod
When running `horovod_nyctaxi.py`, do not use `horovodrun`. Check [here](https://horovod.readthedocs.io/en/stable/ray_include.html) for more information.