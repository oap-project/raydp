import sys
import pytest
import ray
import raydp
from ray.cluster_utils import Cluster
from ray.util.state import list_actors


def test_spark_master_memory_custom(jdk17_extra_spark_configs):
    cluster = Cluster(
        initialize_head=True,
        head_node_args={
            "num_cpus": 2,
            "resources": {"master": 10},
            "include_dashboard": True,
            "dashboard_port": 8270,
        },
    )
    ray.init(address=cluster.address, 
             dashboard_port=cluster.head_node.dashboard_grpc_port,
             include_dashboard=True)

    custom_memory = 100 * 1024 * 1024  # 100MB in bytes
    configs = jdk17_extra_spark_configs.copy()
    # Config under test: set Spark Master actor memory via RayDP config
    configs["spark.ray.raydp_spark_master.actor.resource.memory"] = str(custom_memory)
    # Also require the master custom resource so the actor is scheduled on the head
    configs["spark.ray.raydp_spark_master.actor.resource.master"] = "1"

    app_name = "test_spark_master_memory_custom"

    spark = raydp.init_spark(
        app_name=app_name,
        num_executors=1,
        executor_cores=1,
        executor_memory="500M",
        configs=configs,
    )

    # Trigger the Spark master / RayDPSparkMaster startup
    spark.createDataFrame([(1, 2)], ["a", "b"]).count()

    # RayDPSparkMaster name is app_name + RAYDP_SPARK_MASTER_SUFFIX
    master_actor_name = f"{app_name}_SPARK_MASTER"
    
    actor = ray.get_actor(master_actor_name)
    assert actor is not None

    # Query Ray state for this actor
    actor_state = list_actors(filters=[("actor_id", "=", actor._actor_id.hex())], detail=True)[0]
    resources = actor_state.required_resources
    
    assert resources["memory"] == custom_memory
    assert resources["master"] == 1

    spark.stop()
    raydp.stop_spark()
    ray.shutdown()
    cluster.shutdown()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))


