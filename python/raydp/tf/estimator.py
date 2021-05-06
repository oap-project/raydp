#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import json
import os
from typing import Any, List, NoReturn, Optional, Union

import tensorflow as tf
import tensorflow.keras as keras
from ray.util.data import MLDataset
from ray.util.sgd.tf import TFTrainer
from ray.util.sgd.tf.tf_dataset import TFMLDataset
from tensorflow import DType, TensorShape

from raydp.estimator import EstimatorInterface
from raydp.spark import RayMLDataset
from raydp.spark.interfaces import SparkEstimatorInterface, DF, OPTIONAL_DF


class TFEstimator(EstimatorInterface, SparkEstimatorInterface):
    def __init__(self,
                 num_workers: int = 1,
                 model: keras.Model = None,
                 optimizer: Union[keras.optimizers.Optimizer, str] = None,
                 loss: Union[keras.losses.Loss, str] = None,
                 metrics: Union[List[keras.metrics.Metric], List[str]] = None,
                 feature_columns: Union[str, List[str]] = None,
                 feature_types: Optional[Union[DType, List[DType]]] = None,
                 feature_shapes: Optional[Union[TensorShape, List[TensorShape]]] = None,
                 label_column: str = None,
                 label_type: Optional[tf.DType] = None,
                 label_shape: Optional[tf.TensorShape] = None,
                 batch_size: int = 128,
                 num_epochs: int = 1,
                 shuffle: bool = True,
                 **extra_config):
        """A scikit-learn like API to distributed training Tensorflow Keras model.

        In the backend it leverage the ray.sgd.TorchTrainer.
        :param num_workers: the number of workers for distributed model training
        :param model: the model, it should be instance of tensorflow.keras.Model. We do not support
                      multiple output models.
        :param optimizer: the optimizer, it should be keras.optimizers.Optimizer instance or str.
                          We do not support multiple optimizers currently.
        :param loss: the loss, it should be keras.losses.Loss instance or str. We do not support
                     multiple losses.
        :param metrics: the metrics list. It could be None, a list of keras.metrics.Metric instance
                        or a list of str.
        :param feature_columns: the feature columns name.
               The inputs of the model will be match the feature columns.
               .. code-block:: python
                   feature_columns = ["x", "y", "z"]
                   # the input to the model will be (x_batch_tensor, y_batch_tensor, z_batch_tensor)
        :param feature_types: the type for each feature input. It must match the length of the
                              feature_columns if provided. It will be tf.float32 by default.
        :param feature_shapes: the shape for each feature input. It must match the length of the
                               feature_columns
        :param label_column: the label column name.
        :param label_type: the label type, it will be tf.float32 by default.
        :param label_shape: the label shape.
        :param batch_size: the batch size
        :param num_epochs: the number of epochs
        :param shuffle: whether input dataset should be shuffle, True by default.
        :param extra_config: extra config will fit into TFTrainer. You can also set
               the get_shard config with
               {"get_shard": {batch_ms=0, num_async=5, shuffle_buffer_size=2, seed=0}}.
               You can refer to the MLDataset.get_repeatable_shard for the parameters.
        """
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
        self._batch_size = batch_size
        self._extra_config = extra_config
        self._shuffle = shuffle

        config = {"batch_size": self._batch_size, "shuffle": shuffle}
        if self._extra_config:
            if "config" in self._extra_config:
                self._extra_config["config"].update(config)
            else:
                self._extra_config["config"] = config
        else:
            self._extra_config = {"config": config}

        self._num_epochs: int = num_epochs

        self._trainer: TFTrainer = None

    def _create_tf_ds(self, ds: MLDataset) -> TFMLDataset:
        return ds.to_tf(self._feature_columns,
                        self._feature_shapes,
                        self._feature_types,
                        self._label_column,
                        self._label_shape,
                        self._label_type)

    def fit(self,
            train_ds: MLDataset,
            evaluate_ds: Optional[MLDataset] = None) -> NoReturn:
        super().fit(train_ds, evaluate_ds)

        def model_creator(config):
            # https://github.com/ray-project/ray/issues/5914
            import tensorflow.keras as keras  # pylint: disable=C0415, W0404

            model: keras.Model = keras.models.model_from_json(self._serialized_model)
            optimizer = keras.optimizers.get(self._serialized_optimizer)
            loss = keras.losses.get(self._serialized_loss)
            metrics = [keras.metrics.get(m) for m in self._serialized_metrics]
            model.compile(optimizer=optimizer, loss=loss, metrics=metrics)
            return model

        train_ds = train_ds.batch(self._batch_size)
        train_tf_ds = self._create_tf_ds(train_ds)

        if evaluate_ds is not None:
            evaluate_ds = evaluate_ds.batch(self._batch_size)
            evaluate_tf_ds = self._create_tf_ds(evaluate_ds)
        else:
            evaluate_tf_ds = None

        def data_creator(config):
            if "TF_CONFIG" in os.environ:
                tf_config = json.loads(os.environ["TF_CONFIG"])
                world_rank = tf_config["task"]["index"]
            else:
                world_rank = -1
            batch_size = config["batch_size"]
            get_shard_config = config.get("get_shard", {})
            if "shuffle" in config:
                get_shard_config["shuffle"] = config["shuffle"]
            train_data = train_tf_ds.get_shard(
                world_rank, **get_shard_config).repeat().batch(batch_size)
            options = tf.data.Options()
            options.experimental_distribute.auto_shard_policy = \
                tf.data.experimental.AutoShardPolicy.OFF
            train_data = train_data.with_options(options)
            evaluate_data = None
            if evaluate_tf_ds is not None:
                evaluate_data = evaluate_tf_ds.get_shard(
                    world_rank, **get_shard_config).batch(batch_size)
                evaluate_data = evaluate_data.with_options(options)
            return train_data, evaluate_data

        self._trainer = TFTrainer(model_creator=model_creator,
                                  data_creator=data_creator,
                                  num_replicas=self._num_workers,
                                  **self._extra_config)
        for i in range(self._num_epochs):
            stats = self._trainer.train()
            print(f"Epoch-{i}: {stats}")

        if evaluate_tf_ds is not None:
            print(self._trainer.validate())

    def fit_on_spark(self,
                     train_df: DF,
                     evaluate_df: OPTIONAL_DF = None,
                     fs_directory: Optional[str] = None,
                     compression: Optional[str] = None) -> NoReturn:
        super().fit_on_spark(train_df, evaluate_df)
        train_df = self._check_and_convert(train_df)
        if evaluate_df is not None:
            evaluate_df = self._check_and_convert(evaluate_df)
        train_ds = RayMLDataset.from_spark(
            train_df, self._num_workers, self._shuffle, None, fs_directory, compression)
        evaluate_ds = None
        if evaluate_df is not None:
            evaluate_ds = RayMLDataset.from_spark(
                evaluate_df, self._num_workers, self._shuffle, None, fs_directory, compression)
        return self.fit(train_ds, evaluate_ds)

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
