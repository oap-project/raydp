import json
import os
from typing import List, Union

import numpy as np
import pandas as pd
import tensorflow as tf

from raydp.spark.context import save_to_ray
from raydp.spark.resource_manager.spark_cluster import SharedDataset
from raydp.spark.utils import divide_blocks


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
        self._resolved_data_set: SharedDataset = None

        self._check_and_convert()

    def _check_and_convert(self):
        # convert to list for convenience
        if not isinstance(self._feature_columns, List):
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

    def _create_dataset_from_pandas(self, df: pd.DataFrame) -> tf.data.Dataset:
        tensors: List[tf.Tensor] = []
        feature_shapes = [shape.as_list() for shape in self._feature_shapes]
        [shape.insert(0, -1) for shape in feature_shapes]
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
        pass


class PandasDataset(_Dataset):
    def __init__(self,
                 df: pd.DataFrame,
                 feature_columns: List[str],
                 feature_types: List[tf.DType],
                 feature_shapes: List[tf.TensorShape],
                 label_column: str,
                 label_type: tf.DType,
                 label_shape: tf.TensorShape,
                 shuffle: bool):
        super(PandasDataset, self).__init__(
            feature_columns, feature_types, feature_shapes, label_column,
            label_type, label_shape, shuffle)
        self._df = df

    def setup(self, config) -> tf.data.Dataset:
        batch_size = config["batch_size"]
        return self._create_dataset_from_pandas(self._df).batch(batch_size)


class RayDataset(_Dataset):
    # TODO: currently, we do not support multiple outputs model
    def __init__(self,
                 df: Union['pyspark.sql.DataFrame', 'koalas.DataFrame'],
                 feature_columns: List[str],
                 feature_types: List[tf.DType],
                 feature_shapes: List[tf.TensorShape],
                 label_column: str,
                 label_type: tf.DType,
                 label_shape: tf.TensorShape,
                 shuffle: bool):
        """
        Transfer Spark DataFrame to Tensorflow Dataset
        :param df: the Spark DataFrame or koalas DataFrame
        :param feature_columns: the feature columns, also it is the Model input name
        :param feature_types: the type requirements for the given Model input
        :param feature_shapes: the shape requirements for the given Model input
        :param label_column: the label column
        :param label_type: the label type
        :param label_shape: the label shape
        :param shuffle: whether shuffle the data set
        """
        super(RayDataset, self).__init__(
            feature_columns, feature_types, feature_shapes, label_column,
            label_type, label_shape, shuffle)
        self._data_set: SharedDataset = save_to_ray(df)

    def setup(self, config) -> tf.data.Dataset:
        is_distributed: bool = False
        if "TF_CONFIG" in os.environ:
            is_distributed = True

        if is_distributed:
            dataset = self._setup_distributed_dataset()
        else:
            dataset = self._setup_single_node()
        batch_size = config["batch_size"]
        dataset = dataset.repeat().batch(batch_size)
        return dataset

    def _setup_single_node(self) -> tf.data.Dataset:
        self._resolved_data_set = self._data_set
        self._resolved_data_set.resolve()
        self._resolved = True

        datasets: List[tf.data.Dataset] = []
        # we assume the SharedDataset is not the subset
        partition_sizes = self._resolved_data_set.partition_sizes()
        for i in range(len(partition_sizes)):
            pdf = self._resolved_data_set[i]
            dataset = self._create_dataset_from_pandas(pdf)
            datasets.append(dataset)

        assert len(datasets) > 0
        # concat
        result = datasets[0]
        for i in range(1, len(datasets)):
            result.concatenate(datasets[i])

        if self._shuffle:
            result = result.shuffle()

        return result

    def _setup_distributed_dataset(self) -> tf.data.Dataset:
        tf_config = json.loads(os.environ["TF_CONFIG"])
        world_size = len(tf_config["cluster"]["worker"])
        world_rank = tf_config["task"]["index"]
        blocks, block_sizes = divide_blocks(
            self._data_set.partition_sizes(), world_size, world_rank, self._shuffle, False)
        self._resolved_data_set: SharedDataset = self._data_set.subset(blocks)
        self._resolved_data_set.resolve()
        self._resolved = True

        outer = self

        def make_generator():
            indexes = list(range(len(blocks)))
            if outer._shuffle:
                np.random.shuffle(indexes)
            for i in indexes:
                block_index = blocks[i]
                pdf: pd.DataFrame = outer._data_set[block_index]
                features = [pdf[col].values for col in outer._feature_columns]
                label = pdf[outer._label_column].values
                inner_indexes = list(range(block_sizes[i]))
                if outer._shuffle:
                    np.random.shuffle(inner_indexes)
                for j in inner_indexes:
                    results = [f[j] for f in features]
                    yield tuple(results), label[j]

        output_shapes = self._feature_shapes.copy()
        output_shapes = (tuple(output_shapes), self._label_shape)

        output_types = self._feature_types.copy()
        output_types = (tuple(output_types),  self._label_type)

        return tf.data.Dataset.from_generator(generator=make_generator,
                                              output_types=output_types,
                                              output_shapes=output_shapes)
