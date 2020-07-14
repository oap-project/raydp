from typing import List

import tensorflow as tf
import ray

from raydp.spark.cluster.standalone.block_holder import BlockSet


# TODO: wrap into Dataset class
def create_dataset_from_objects(
        block_set: BlockSet,
        features_columns: List[str],
        label_column: str) -> tf.data.Dataset:
    # TODO: this will load all data into memory which is not optimized.
    block_set.resolve(indices=None)
    assert ray.is_initialized()
    plasma_store_path = ray.worker.global_worker.node.plasma_store_socket_name
    block_set.set_plasma_store_socket_name(plasma_store_path)
    # transfer to Dataset
    datasets: List[tf.data.Dataset] = []
    for i in range(len(block_set.block_sizes)):
        pdf = block_set[i]
        dataset = tf.data.Dataset.from_tensor_slices((pdf[features_columns].values,
                                                      pdf[label_column].values))
        datasets.append(dataset)

    assert len(datasets) > 0
    # concat
    result = datasets[0]
    for i in range(1, len(datasets)):
        result.concatenate(datasets[i])

    return result
