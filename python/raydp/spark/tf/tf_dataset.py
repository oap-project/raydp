from raydp.spark.block_holder import BlockSet
from typing import Dict, List

import tensorflow as tf


# TODO: wrap into Dataset class
def create_dataset_from_objects(
        block_set: BlockSet,
        features_columns: List[str],
        label_column: str) -> tf.data.Dataset:
    # TODO: this will load all data into memory which is not optimized.
    block_set.resolve(indices=None, batch=True)
    # transfer to Dataset
    datasets: List[tf.data.Dataset] = \
        [tf.data.Dataset.from_tensor_slices((pdf[features_columns].values, pdf[label_column].values))
         for pdf in block_set]
    assert len(datasets) > 0
    # concat
    result = datasets[0]
    for i in range(1, len(datasets)):
        result.concatenate(datasets[i])

    return result
