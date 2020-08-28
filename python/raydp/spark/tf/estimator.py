from typing import Any, Dict, List, NoReturn, Optional, Union

import tensorflow as tf
import tensorflow.keras as keras
from tensorflow import DType, TensorShape
from ray.util.sgd.tf import TFTrainer

from raydp.spark.estimator import EstimatorInterface
from raydp.spark.tf.dataset import PandasDataset, RayDataset


class TFEstimator(EstimatorInterface):
    """
    A scikit-learn like API to distributed training Tensorflow Keras model. In the backend it
    leverage the ray.sgd.TorchTrainer.
    """
    def __init__(self,
                 num_workers: int = 1,
                 model: keras.Model = None,
                 optimizer: Union[keras.optimizers.Optimizer, str] = None,
                 loss: Union[keras.losses.Loss, str] = None,
                 metrics: Union[List[keras.metrics.Metric], List[str]] = None,
                 feature_columns: Union[str, List[str]] = None,
                 feature_types: Union[Optional[DType, Optional[List[DType]]]] = tf.float32,
                 feature_shapes: Union[Optional[TensorShape], Optional[List[TensorShape]]] = None,
                 label_column: str = None,
                 label_type: Optional[tf.DType] = tf.float32,
                 label_shape: Optional[tf.TensorShape] = None,
                 batch_size: int = None,
                 num_epochs: int = None,
                 shuffle: bool = True,
                 config: Dict = None):
        self._num_workers: int = num_workers

        # model
        assert model is not None, "model must be not be None"
        if isinstance(model, keras.Model):
            self._serialized_model = model.to_json()
        else:
            raise Exception("Unsupported parameter, we only support tensorflow.keras.Model")

        # optimizer
        # TODO: we should support multiple optimizers for multiple outputs model
        assert optimizer is not None, "optimizer must not be None"
        if isinstance(optimizer, str):
            # it is a str represents the optimizer
            _optimizer = optimizer
        elif isinstance(optimizer, keras.optimizers.Optimizer):
            _optimizer = keras.optimizers.serialize(optimizer)
        else:
            raise Exception(
                "Unsupported parameter, we only support keras.optimizers.Optimizer subclass "
                "instance or a str to represent the optimizer")
        self._serialized_optimizer = _optimizer

        # loss
        # TODO: we should support multiple losses for multiple outputs model
        assert loss is not None, "loss must not be None"
        if isinstance(loss, str):
            _loss = loss
        elif isinstance(loss, keras.losses.Loss):
            _loss = keras.losses.serialize(loss)
        else:
            raise Exception(
                "Unsupported parameter, we only support keras.losses.Loss subclass "
                "instance or a str to represents the loss)")
        self._serialized_loss = _loss

        # metrics
        if metrics is None:
            _metrics = None
        else:
            assert isinstance(metrics, list), "metrics must be a list"
            if isinstance(metrics[0], str):
                _metrics = metrics
            elif isinstance(metrics[0], keras.metrics.Metric):
                _metrics = [keras.metrics.serialize(m) for m in metrics]
            else:
                raise Exception(
                    "Unsupported parameter, we only support list of "
                    "keras.metrics.Metrics instances or list of str to")
        self._serialized_metrics = _metrics

        self._feature_columns = feature_columns
        self._feature_types = feature_types
        self._feature_shapes = feature_shapes
        self._label_column = label_column
        self._label_type = label_type
        self._label_shape = label_shape

        _config = {"batch_size": batch_size}
        _config.update(config)
        self._config = _config
        self._num_epochs: int = num_epochs
        self._shuffle: bool = shuffle

        self._trainer: TFTrainer = None

    def fit(self, df, **kwargs) -> NoReturn:
        super(TFEstimator, self).fit(df, **kwargs)

        def create_model(config):
            model: keras.Model = keras.models.model_from_json(self._serialized_model)
            optimizer = keras.optimizers.get(self._serialized_optimizer)
            loss = keras.losses.get(self._serialized_loss)
            metrics = keras.metrics.get(self._serialized_metrics)
            model.compile(optimizer=optimizer, loss=loss, metrics=metrics)
            model.fit()
            return model

        data_set = RayDataset(df,
                              self._feature_columns,
                              self._feature_types,
                              self._feature_shapes,
                              self._label_column,
                              self._label_type,
                              self._label_shape,
                              self._shuffle)

        def data_create(config):
            return data_set.setup(config), None

        self._trainer = TFTrainer(create_model, data_create, self._config, self._num_workers)
        for i in range(self._num_epochs):
            stats = self._trainer.train()
            print(f"Epoch-{i}: {stats}")

    def evaluate(self, df, **kwargs) -> NoReturn:
        super(TFEstimator, self).evaluate(df)
        if self._trainer is None:
            raise Exception("Must call fit first")
        pdf = df.toPandas()
        dataset = PandasDataset(pdf,
                                self._feature_columns,
                                self._feature_types,
                                self._feature_shapes,
                                self._label_column,
                                self._label_type,
                                self._label_shape,
                                self._shuffle)
        config = self._config
        tf_dataset = dataset.setup(config)
        model = self._trainer.get_model()
        result = model.fit(tf_dataset)
        print(result)

    def get_model(self) -> Any:
        assert self._trainer, "Trainer has not been created"
        return self._trainer.get_model()

    def save(self, file_path) -> NoReturn:
        assert self._trainer, "Trainer has not been created"
        self._trainer.save(file_path)

    def restore(self, file_path) -> NoReturn:
        assert self._trainer, "Trainer has not been created"
        self._trainer.restore(file_path)

    def shutdown(self) -> NoReturn:
        if self._trainer is not None:
            self._trainer.shutdown()
            del self._trainer

