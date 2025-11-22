
import sys
import pytest
import ray
import raydp

def test_app_master_memory_default(ray_cluster, jdk17_extra_spark_configs):
    # Initialize Spark with default settings
    spark = raydp.init_spark(
        app_name="test_app_master_memory",
        num_executors=1,
        executor_cores=1,
        executor_memory="500M",
        configs=jdk17_extra_spark_configs
    )

    spark.createDataFrame([(1, 2)], ["a", "b"]).count()
    
    # Find the AppMaster actor
    actor = ray.get_actor("RAY_APP_MASTER")
    assert actor is not None
    
    # Get actor metadata from Ray state
    actor_info = ray.state.actors(actor._actor_id.hex())
    
    # Verify memory requirement
    resources = actor_info["RequiredResources"]
    
    # Default memory should be 500MB (524288000 bytes)
    expected_memory = 500 * 1024 * 1024
    assert resources.get("memory", 0) == expected_memory



def test_app_master_memory_custom(ray_cluster, jdk17_extra_spark_configs):
    # Initialize Spark with custom memory for AppMaster
    # 100MB = 104857600 bytes
    custom_memory = 100 * 1024 * 1024
    configs = jdk17_extra_spark_configs.copy()
    configs["spark.ray.raydp_app_master.memory"] = str(custom_memory)
    
    spark = raydp.init_spark(
        app_name="test_app_master_memory_custom",
        num_executors=1,
        executor_cores=1,
        executor_memory="500M",
        configs=configs
    )

    spark.createDataFrame([(1, 2)], ["a", "b"]).count()
    
    # Find the AppMaster actor
    actor = ray.get_actor("RAY_APP_MASTER")
    
    # Get actor metadata from Ray state
    actor_info = ray.state.actors(actor._actor_id.hex())
    resources = actor_info["RequiredResources"]
    
    assert resources.get("memory", 0) == custom_memory


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
