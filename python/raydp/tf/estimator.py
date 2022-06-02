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

from typing import Any, List, NoReturn, Optional, Union

import tensorflow as tf
import tensorflow.keras as keras
from tensorflow import DType, TensorShape
from tensorflow.keras.callbacks import Callback
from ray import train
from ray.train import Trainer
from ray.train.tensorflow import prepare_dataset_shard
from ray.data.dataset import Dataset
from ray.data.impl.arrow_block import ArrowRow
from raydp.estimator import EstimatorInterface
from raydp.spark.interfaces import SparkEstimatorInterface, DF, OPTIONAL_DF
from raydp import stop_spark
from raydp.spark import spark_dataframe_to_ray_dataset

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
                 callbacks: Optional[List[Callback]] = None,
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
        :param callbacks: which will be executed during training.
        :param extra_config: extra config will fit into TFTrainer. You can also set
               the get_shard config with
               {"get_shard": {batch_ms=0, num_async=5, shuffle_buffer_size=2, seed=0}}.
               You can refer to the MLDataset.get_repeatable_shard for the parameters.
        """
        self._num_workers: int = num_workers
        self._model = model
        self._num_workers = num_workers
        self._model = model
        self._optimizer = optimizer
        self._loss = loss
        self._metrics = metrics
        self._feature_columns = feature_columns
        self._feature_types = feature_types
        self._feature_shapes = feature_shapes
        self._label_column = label_column
        self._label_shape = label_shape
        self._label_type = label_type
        self._batch_size = batch_size
        self._num_epochs = num_epochs
        self._shuffle = shuffle
        self._callbacks = callbacks
        self._extra_config = extra_config
        self._trainer: Trainer = None
        self._check()

    def _check(self):
        assert self._model is not None, "Model must be provided"
        assert isinstance(self._model, keras.Model), "Unsupported parameter, "\
                                    "we only support tensorflow.keras.Model"
        assert self._optimizer is not None, "Optimizer must be provided"
        assert self._loss is not None, "Loss must be provided"

    @staticmethod
    def build_and_compile_model(config):
        # create model
        model: keras.Model = keras.models.model_from_json(config["model"])

        # create optimizer
        # TODO: we should support multiple optimizers for multiple outputs model
        if isinstance(config["optimizer"], str):
            # it is a str represents the optimizer
            serialized_optimizer = config["optimizer"]
        elif isinstance(config["optimizer"], keras.optimizers.Optimizer):
            serialized_optimizer = keras.optimizers.serialize(config["optimizer"])
        else:
            raise Exception(
                "Unsupported parameter, we only support keras.optimizers.Optimizer subclass "
                "instance or a str to represent the optimizer")
        optimizer = keras.optimizers.get(serialized_optimizer)

        # create loss
        # TODO: we should support multiple losses for multiple outputs model
        if isinstance(config["loss"], str):
            serialized_loss = config["loss"]
        elif isinstance(config["loss"], keras.losses.Loss):
            serialized_loss = keras.losses.serialize(config["loss"])
        else:
            raise Exception(
                "Unsupported parameter, we only support keras.losses.Loss subclass "
                "instance or a str to represents the loss)")
        loss = keras.losses.get(serialized_loss)

        # create metrics
        if "metrics" not in config:
            serialized_metrics = None
        else:
            assert isinstance(config["metrics"], list), "metrics must be a list"
            if isinstance(config["metrics"][0], str):
                serialized_metrics = config["metrics"]
            elif isinstance(config["metrics"][0], keras.metrics.Metric):
                serialized_metrics = [keras.metrics.serialize(m) for m in config["metrics"]]
            else:
                raise Exception(
                    "Unsupported parameter, we only support list of "
                    "keras.metrics.Metrics instances or list of str to")
        metrics = [keras.metrics.get(m) for m in serialized_metrics]

        model.compile(optimizer=optimizer, loss=loss, metrics=metrics)
        return model

    @staticmethod
    def train_fun(config):
        strategy = tf.distribute.MultiWorkerMirroredStrategy()
        with strategy.scope():
            # Model building/compiling need to be within `strategy.scope()`.
            multi_worker_model = TFEstimator.build_and_compile_model(config)

        train_dataset_pipeline = train.get_dataset_shard("train")
        train_dataset_iterator = train_dataset_pipeline.iter_epochs()
        eval_dataset_pipeline = train.get_dataset_shard("evaluate")
        eval_dataset_iterator = eval_dataset_pipeline.iter_epochs()

        results = []
        for _ in range(config["num_epochs"]):
            features_len = len(config["feature_columns"])
            dataset = next(train_dataset_iterator)
            tf_dataset = prepare_dataset_shard(
                dataset.to_tf(
                    label_column=config["label_column"],
                    output_signature=(
                        tf.TensorSpec(shape=(None, features_len), dtype=tf.float32),
                        tf.TensorSpec(shape=(None), dtype=tf.float32)
                    ),
                    batch_size=config["batch_size"],
                )
            )
            eval_dataset = next(eval_dataset_iterator)
            eval_tf_dataset = prepare_dataset_shard(
                eval_dataset.to_tf(
                    label_column=config["label_column"],
                    output_signature=(
                        tf.TensorSpec(shape=(None, features_len), dtype=tf.float32),
                        tf.TensorSpec(shape=(None), dtype=tf.float32)
                    ),
                    batch_size=config["batch_size"],
                )
            )
            callbacks = config["callbacks"]
            train_history = multi_worker_model.fit(tf_dataset, callbacks=callbacks)
            results.append(train_history.history)
            test_history = multi_worker_model.evaluate(eval_tf_dataset, callbacks=callbacks)
            results.append(test_history)
        return results

    def fit(self,
            train_ds: Dataset[ArrowRow],
            evaluate_ds: Optional[Dataset[ArrowRow]] = None,
            max_retries=3) -> NoReturn:
        super().fit(train_ds, evaluate_ds)
        self._trainer = Trainer(backend="tensorflow", num_workers=self._num_workers,
                                max_retries=max_retries, **self._extra_config)

        config = {"num_workers": self._num_workers,
                  "model": self._model.to_json(),
                  "optimizer": self._optimizer,
                  "loss": self._loss,
                  "feature_columns": self._feature_columns,
                  "label_column": self._label_column,
                  "batch_size": self._batch_size,
                  "num_epochs": self._num_epochs,
                  "evaluate": True,
                  "metrics": self._metrics,
                  "callbacks": self._callbacks
                  }

        train_ds_pipeline = train_ds.repeat()
        if self._shuffle:
            train_ds_pipeline = train_ds_pipeline.random_shuffle_each_window()
        dataset = {"train": train_ds_pipeline}
        if evaluate_ds is None:
            config["evaluate"] = False
        else:
            evaluate_ds_pipeline = evaluate_ds.repeat()
            if self._shuffle:
                evaluate_ds_pipeline = evaluate_ds_pipeline.random_shuffle_each_window()
            dataset["evaluate"] = evaluate_ds_pipeline
        self._trainer.start()
        results = self._trainer.run(
            train_func=TFEstimator.train_fun,
            dataset=dataset,
            config=config,
        )
        return results

    def fit_on_spark(self,
                     train_df: DF,
                     evaluate_df: OPTIONAL_DF = None,
                     max_retries=3,
                     stop_spark_after_conversion=False) -> NoReturn:
        super().fit_on_spark(train_df, evaluate_df)
        train_df = self._check_and_convert(train_df)
        train_ds = spark_dataframe_to_ray_dataset(train_df,
                                                  parallelism=self._num_workers,
                                                  _use_owner=stop_spark_after_conversion)

        evaluate_ds = None
        if evaluate_df is not None:
            evaluate_df = self._check_and_convert(evaluate_df)
            evaluate_ds = spark_dataframe_to_ray_dataset(evaluate_df,
                                                         parallelism=self._num_workers,
                                                         _use_owner=stop_spark_after_conversion)

        if stop_spark_after_conversion:
            stop_spark(del_obj_holder=False)
        return self.fit(
            train_ds, evaluate_ds, max_retries)

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
