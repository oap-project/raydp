from raydp.spark.dataholder import DataHolderActorHandlerWrapper, ObjectIdList
from raydp.spark.spark_cluster import _global_data_holder
from typing import Dict, List

import tensorflow as tf


# TODO: wrap into Dataset class
def create_dataset_from_objects(
        objs: ObjectIdList,
        features_columns: List[str],
        label_column: str) -> tf.data.Dataset:
    # TODO: this will load all data into memory which is not optimized.
    objs.resolve(True)
    # transfer to Dataset
    datasets: List[tf.data.Dataset] = \
        [tf.data.Dataset.from_tensor_slices((pdf[features_columns].values, pdf[label_column].values))
         for pdf in objs]
    assert len(datasets) > 0
    # concat
    result = datasets[0]
    for i in range(1, len(datasets)):
        result.concatenate(datasets[i])

    return result
