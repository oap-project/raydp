import json
import os
from typing import Any, List

import numpy as np
import pandas as pd
import ray
import tensorflow as tf
from tensorflow.data import Dataset

from raydp.spark.context import save_to_ray
from raydp.spark.resource_manager.spark_cluster import SharedDataset
from raydp.spark.utils import divide_blocks


class DistributedDataset:
    # TODO: currently, we do not support multiple inputs model
    def __init__(self,
                 df: Any,
                 feature_columns: List[str],
                 feature_type: tf.DType,
                 label_column: str,
                 label_type: tf.DType,
                 shuffle: bool):
        self._data_set: SharedDataset = save_to_ray(df)
        self._feature_columns: List[str] = feature_columns
        self._feature_type: tf.DType = feature_type
        self._label_column: str = label_column
        self._label_type: tf.DType = label_type
        self._shuffle: bool = shuffle
        self._resolved: bool = False
        self._resolved_data_set: SharedDataset = None

    def setup(self, config) -> Dataset:
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

    def _setup_single_node(self) -> Dataset:
        self._resolved_data_set = self._data_set
        self._resolved_data_set.resolve()
        self._resolved = True

        assert ray.is_initialized()

        datasets: List[tf.data.Dataset] = []
        # we assume the SharedDataset is not the subset
        partition_sizes = self._resolved_data_set.partition_sizes()
        for i in range(len(partition_sizes)):
            pdf = self._resolved_data_set[i]
            dataset = tf.data.Dataset.from_tensor_slices((pdf[self._feature_columns].values,
                                                          pdf[self._label_column].values))
            datasets.append(dataset)

        assert len(datasets) > 0
        # concat
        result = datasets[0]
        for i in range(1, len(datasets)):
            result.concatenate(datasets[i])

        if self._shuffle:
            result = result.shuffle()

        return result

    def _setup_distributed_dataset(self) -> Dataset:
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
            indexes = range(len(blocks))
            if outer._shuffle:
                np.random.shuffle(indexes)
            for i in indexes:
                block_index = blocks[i]
                pdf: pd.DataFrame = outer._data_set[block_index]
                features = pdf[outer._feature_columns].values
                label = pdf[outer._label_column].values
                inner_indexes = range(block_sizes[i])
                if outer._shuffle:
                    np.random.shuffle(inner_indexes)
                for j in inner_indexes:
                    yield features[j], label[j]

        return Dataset.from_generator(generator=make_generator,
                                      output_types=(outer._feature_type, outer._label_type))
