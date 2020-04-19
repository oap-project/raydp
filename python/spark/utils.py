from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import BinaryType
from typing import List

import pandas as pd
import ray
import ray.cloudpickle as rpickle


@pandas_udf(BinaryType())
def save_to_ray(*columns) -> List[bytes]:
    if not ray.is_initialized():
        ray.init(address="auto", redis_password="123")
    df = pd.concat(columns, axis=1, copy=False)
    id = ray.put(df)

    id_bytes = rpickle.dumps(id)
    return id_bytes


def load_into_ids(id_in_bytes: List[bytes]) -> List[ray.ObjectID]:
    return [rpickle.loads(i) for i in id_in_bytes]
