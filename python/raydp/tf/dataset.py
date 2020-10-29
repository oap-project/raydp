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
from typing import Callable, List, Optional

import pandas as pd
import tensorflow as tf

from raydp.parallel import IteratorShard
from raydp.parallel import PandasDataset as ParallelPandasDataset


class _Dataset:
    def __init__(self,
                 feature_columns: List[str],
                 feature_types: List[tf.DType],
                 feature_shapes: List[tf.TensorShape],
                 label_column: str,
                 label_type: tf.DType,
                 label_shape: tf.TensorShape,
                 shuffle: bool):

        self._feature_columns: List[str] = feature_columns
        self._feature_types: List[tf.DType] = feature_types
        self._feature_shapes: List[tf.TensorShape] = feature_shapes
        self._label_column: str = label_column
        self._label_type: tf.DType = label_type
        self._label_shape: tf.TensorShape = label_shape
        self._shuffle: bool = shuffle
        self._resolved: bool = False

        self._check_and_convert()

    def _check_and_convert(self):
        # convert to list for convenience
        if not isinstance(self._feature_columns, list):
            self._feature_columns = [self._feature_columns]

        if self._feature_shapes:
            if not isinstance(self._feature_shapes, list):
                self._feature_shapes = [self._feature_shapes]

            assert len(self._feature_columns) == len(self._feature_shapes), \
                "The feature_shapes size must match the feature_columns"

        if self._feature_types:
            if not isinstance(self._feature_types, list):
                self._feature_types = [self._feature_types]

            assert len(self._feature_columns) == len(self._feature_types), \
                "The feature_types size must match the feature_columns"
            for i in range(len(self._feature_types)):
                assert all(isinstance(dtype, tf.DType) for dtype in self._feature_types), \
                    "All value in feature_types should be tf.DType instance"

        if not self._feature_shapes:
            self._feature_shapes = [tf.TensorShape(([]))] * len(self._feature_columns)

        if not self._feature_types:
            self._feature_types = [tf.float32] * len(self._feature_columns)

        if not self._label_type:
            self._label_type = tf.float32

        if not self._label_shape:
            self._label_shape = tf.TensorShape(([]))

    def setup(self, config) -> tf.data.Dataset:
        pass


class PandasTFDataset(_Dataset):
    def __init__(self,
                 df: pd.DataFrame,
                 feature_columns: List[str],
                 feature_types: List[tf.DType],
                 feature_shapes: List[tf.TensorShape],
                 label_column: str,
                 label_type: tf.DType,
                 label_shape: tf.TensorShape,
                 shuffle: bool):
        super(PandasTFDataset, self).__init__(
            feature_columns, feature_types, feature_shapes, label_column,
            label_type, label_shape, shuffle)
        self._df = df

    def _create_dataset_from_pandas(self, df: pd.DataFrame) -> tf.data.Dataset:
        tensors: List[tf.Tensor] = []
        feature_shapes = [shape.as_list() for shape in self._feature_shapes]
        for shape in feature_shapes:
            shape.insert(0, -1)
        label_shape = self._label_shape.as_list()
        label_shape.insert(0, -1)

        for col, tp, shape in zip(self._feature_columns,
                                  self._feature_types,
                                  feature_shapes):
            col_t = tf.convert_to_tensor(df[col], dtype=tp)
            col_t = tf.reshape(col_t, shape)
            tensors.append(col_t)

        label_tensor = tf.convert_to_tensor(df[self._label_column], self._label_type)
        label_tensor = tf.reshape(label_tensor, label_shape)
        return tf.data.Dataset.from_tensor_slices((tuple(tensors), label_tensor))

    def setup(self, config) -> tf.data.Dataset:
        batch_size = config["batch_size"]
        return self._create_dataset_from_pandas(self._df).batch(batch_size)


class TFDataset(_Dataset):
    # TODO: currently, we do not support multiple outputs model
    def __init__(self,
                 ds: ParallelPandasDataset,
                 feature_columns: List[str],
                 feature_types: List[tf.DType],
                 feature_shapes: List[tf.TensorShape],
                 label_column: str,
                 label_type: tf.DType,
                 label_shape: tf.TensorShape,
                 shuffle: bool):
        """
        :param ds: the raydp.parallel.PandasDataset
        :param feature_columns: the feature columns, also it is the Model input name
        :param feature_types: the type requirements for the given Model input
        :param feature_shapes: the shape requirements for the given Model input
        :param label_column: the label column
        :param label_type: the label type
        :param label_shape: the label shape
        :param shuffle: whether shuffle the data set
        """
        super(TFDataset, self).__init__(
            feature_columns, feature_types, feature_shapes, label_column,
            label_type, label_shape, shuffle)
        # TODO: support shuffle
        self._data_set: ParallelPandasDataset = ds

    def setup(self, config) -> tf.data.Dataset:
        world_rank = None
        if "TF_CONFIG" in os.environ:
            tf_config = json.loads(os.environ["TF_CONFIG"])
            world_size = len(tf_config["cluster"]["worker"])
            assert world_size == self._data_set.num_shards()
            world_rank = tf_config["task"]["index"]

        dataset = self.setup_dataset(world_rank)
        batch_size = config["batch_size"]
        if self._shuffle:
            dataset = dataset.shuffle(buffer_size=batch_size, seed=world_rank)
        dataset = dataset.batch(batch_size).repeat()
        return dataset

    def setup_dataset(self, world_rank: Optional[int]) -> tf.data.Dataset:
        if world_rank is not None:
            it = self._data_set.get_shard(world_rank)
        else:
            it = self._data_set.collect()

        make_generator = self._make_ds_generator(it)
        output_shapes = self._feature_shapes.copy()
        output_shapes = (tuple(output_shapes), self._label_shape)

        output_types = self._feature_types.copy()
        output_types = (tuple(output_types),  self._label_type)

        return tf.data.Dataset.from_generator(generator=make_generator,
                                              output_types=output_types,
                                              output_shapes=output_shapes)

    def _make_ds_generator(self, data: IteratorShard[pd.DataFrame]) -> Callable:
        outer = self

        def make_generator():
            for pdf in iter(data):
                num_rows = pdf.shape[0]
                feature_columns = [pdf[col].values for col in outer._feature_columns]
                label_column = pdf[outer._label_column].values
                for i in range(num_rows):
                    features = [f[i] for f in feature_columns]
                    yield tuple(features), label_column[i]
        return make_generator
