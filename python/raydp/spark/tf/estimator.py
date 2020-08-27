import json
from typing import Any, Callable, Dict, List, NoReturn, Optional, Union

import tensorflow as tf
import tensorflow.keras as keras
from ray.util.sgd.tf import TFTrainer

from raydp.spark.estimator import EstimatorInterface
from raydp.spark.tf.dataset import DistributedDataset


class TFEstimator(EstimatorInterface):
    def __init__(self,
                 num_workers: int = 1,
                 model: Union[keras.Model, Callable] = None,
                 optimizer: Union[keras.optimizers.Optimizer, Callable] = None,
                 loss: Union[keras.losses.Loss, Callable] = None,
                 metrics: List[str] = None,
                 feature_columns: List[str] = None,
                 feature_type: Optional[tf.DType] = tf.float32,
                 label_column: str = None,
                 label_type: Optional[tf.DType] = tf.float32,
                 batch_size: int = None,
                 num_epochs: int = None,
                 shuffle: bool = True,
                 config: Dict = None):
        self._num_workers: int = num_workers

        if callable(model):
            # if this is model create function
            model_create_fn = model
        elif isinstance(model, keras.Model):
            serialized_state = model.to_json()

            def _model_fn(config):
                return keras.models.model_from_json(serialized_state)
            model_create_fn = _model_fn()
        else:
            raise Exception("Unsupported parameter, we only support tensorflow.keras.Model "
                            "instance or a function(dict -> model)")
        self._model_create_fn = model_create_fn

        if callable(optimizer):
            optimizer_create_fn = optimizer
        elif isinstance(optimizer, keras.optimizers.Optimizer):
            optimizer_state = keras.optimizers.serialize(optimizer)
            optimizer_state_in_json = json.dumps(optimizer_state)

            def _optimizer_fn(config):
                return keras.optimizers.deserialize(json.loads(optimizer_state_in_json))
            optimizer_create_fn = _optimizer_fn
        else:
            raise Exception(
                "Unsupported parameter, we only support keras.optimizers.Optimizer subclass "
                "instance or a function((models, dict) -> optimizer)")
        self._optimizer_create_fn = optimizer_create_fn

        if callable(loss):
            loss_create_fn = loss
        elif isinstance(loss, keras.losses.Loss):
            loss_state = keras.losses.serialize(loss)
            loss_state_in_json = json.dumps(loss_state)

            def _loss_fn(config):
                return keras.losses.deserialize(json.loads(loss_state_in_json))
            loss_create_fn = _loss_fn
        else:
            raise Exception(
                "Unsupported parameter, we only support keras.losses.Loss subclass "
                "instance or a function((models, dict) -> loss)")
        self._loss_create_fn = loss_create_fn
        self._metrics = metrics

        self._feature_columns: List[str] = feature_columns
        self._feature_type: Optional[tf.DType] = feature_type
        self._label_column: str = label_column
        self._label_type: Optional[tf.DType] = label_type

        _config = {"batch_size": batch_size}
        _config.update(config)
        self._config = _config
        self._num_epochs: int = num_epochs
        self._shuffle: bool = shuffle

        self._trainer: TFTrainer = None

    def fit(self, df) -> NoReturn:
        super(TFEstimator, self).fit(df)

        def create_model(config):
            model: keras.Model = self._model_create_fn(config)
            optimizer: keras.optimizers.Optimizer = self._optimizer_create_fn(config)
            loss: keras.losses.Loss = self._loss_create_fn(config)
            model.compile(optimizer=optimizer, loss=loss, metrics=self._metrics)
            return model

        data_set = DistributedDataset(df,
                                      self._feature_columns,
                                      self._feature_type,
                                      self._label_column,
                                      self._label_type,
                                      self._shuffle)

        def data_create(config):
            return data_set.setup(config), None

        self._trainer = TFTrainer(create_model, data_create, self._config, self._num_workers)
        for i in range(self._num_epochs):
            stats = self._trainer.train()
            print(f"Epoch-{i}: {stats}")

    def evaluate(self, df: Any) -> NoReturn:
        super(TFEstimator, self).evaluate(df)
        if self._trainer is None:
            raise Exception("Must call fit first")
        pdf = df.toPandas()
        dataset = tf.data.Dataset.from_tensor_slices((pdf[self._feature_columns].values,
                                                      pdf[self._label_column].values))
        config = self._config
        model: keras.Model = self._model_create_fn(config)
        optimizer: keras.optimizers.Optimizer = self._optimizer_create_fn(config)
        loss: keras.losses.Loss = self._loss_create_fn(config)
        model.compile(optimizer=optimizer, loss=loss)
        result = model.evaluate(dataset)
        print(result)
