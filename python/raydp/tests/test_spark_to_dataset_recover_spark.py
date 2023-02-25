from typing import Optional

import ray
import raydp
from ray.data import Dataset
from ray.data._internal.arrow_block import ArrowRow


def from_spark(
    df: "pyspark.sql.DataFrame", *,parallelism: Optional[int] = None,
    recover_ray_resources_from_spark=False
) -> Dataset[ArrowRow]:
    """Create a dataset from a Spark dataframe.
    Args:
        spark: A SparkSession, which must be created by RayDP (Spark-on-Ray).
        df: A Spark dataframe, which must be created by RayDP (Spark-on-Ray).
            parallelism: The amount of parallelism to use for the dataset.
            If not provided, it will be equal to the number of partitions of
            the original Spark dataframe.
    Returns:
        Dataset holding Arrow records read from the dataframe.
    """
    import raydp

    return raydp.spark.spark_dataframe_to_ray_dataset(df,
                                                      parallelism,
                                                      recover_ray_resources_from_spark=recover_ray_resources_from_spark)

ray.init()
spark = raydp.init_spark(app_name="RayDP Example",
                         num_executors=2,
                         executor_cores=2,
                         executor_memory="4G")



# Spark Dataframe to Ray Dataset
df1 = spark.range(0, 1000)
raydp.stop_spark(False)
ds1 = from_spark(df1, recover_ray_resources_from_spark=True)

# Ray Dataset to Spark Dataframe
ds2 = ray.data.from_items([{"id": i} for i in range(1000)])
df2 = ds2.to_spark(spark)


"""
my run res is:
after init spark, ray available_resources is:  {'CPU': 2.0, 'memory': 2920706458.0, 'node:127.0.0.1': 1.0, 'object_store_memory': 2147483648.0}
after convert spark df to ray dataset(use recover_ray_resources_from_spark is True),ray available_resources is:  {'CPU': 3.0, 'object_store_memory': 2147483648.0, 'node:127.0.0.1': 1.0, 'memory': 7215673754.0}
after call raydp.stop_spark, ray available_resources is:  {'CPU': 4.0, 'memory': 7215673754.0, 'node:127.0.0.1': 1.0, 'object_store_memory': 2147483648.0}

It can be seen that after adding the parameter recover_ray_resources_from_spark, the resources are indeed released
"""
