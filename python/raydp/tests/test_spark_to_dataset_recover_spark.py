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
def test_():
    
    ray.init()
    init_resources = ray.available_resources()
    spark = raydp.init_spark(app_name="RayDP Example",
                             num_executors=1,
                             executor_cores=1,
                             executor_memory="500M")
    init_spark_resources = ray.available_resources()
    assert (init_resources["CPU"] - init_spark_resources["CPU"]) == 1.0 
    assert (init_resources["memory"] - init_spark_resources["memory"]) == 524288000.0
    # Spark Dataframe to Ray Dataset
    df1 = spark.range(0, 1000)
    
    ds1 = from_spark(df1, recover_ray_resources_from_spark=True)
    
    recover_ray_resources = ray.available_resources()
    assert (recover_ray_resources["CPU"] - init_spark_resources["CPU"]) == 1.0 
    assert (recover_ray_resources["memory"] - init_spark_resources["memory"]) == 524288000.0
    
    raydp.stop_spark(True)
    stop_spark_resources = ray.available_resources()
    assert (stop_spark_resources["CPU"] - init_spark_resources["CPU"]) == 1.0 
    assert (stop_spark_resources["memory"] - init_spark_resources["memory"]) == 524288000.0
    
    # reinit spark
    spark = raydp.init_spark(app_name="RayDP Example1",
                             num_executors=1,
                             executor_cores=1,
                             executor_memory="500M")

    # Ray Dataset to Spark Dataframe
    ds2 = ray.data.from_items([{"id": i} for i in range(1000)])
    df2 = ds2.to_spark(spark)
    
if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
