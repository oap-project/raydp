from raydp.spark.dataholder import DataHolderActorHandlerWrapper, ObjectIdList
from raydp.spark.spark_cluster import _global_data_holder
from typing import Dict, List

import tensorflow as tf


def create_dataset_from_objects(
        objs: ObjectIdList,
        features_columns: List[str],
        label_column: str,
        data_holder_mapping: Dict[str, DataHolderActorHandlerWrapper] = _global_data_holder
) -> tf.data.Dataset:
    # TODO: wrap into Dataset class
    # TODO: this will load all data into memory which is not optimized.
    objs.resolve(data_holder_mapping, True)
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