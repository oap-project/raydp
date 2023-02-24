import ray
import raydp

ray.init()
spark = raydp.init_spark(app_name="RayDP Example",
                         num_executors=2,
                         executor_cores=2,
                         executor_memory="4GB")



# Spark Dataframe to Ray Dataset
df1 = spark.range(0, 1000)
raydp.stop_spark(False)
ds1 = ray.data.from_spark(df1)

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
