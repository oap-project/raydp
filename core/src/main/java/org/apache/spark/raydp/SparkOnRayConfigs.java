package org.apache.spark.raydp;

public class SparkOnRayConfigs {
    public static final String RAY_ACTOR_RESOURCE_PREFIX = "spark.ray.actor.resource";
    /**
     * CPU cores per Ray Actor which host the Spark executor, the resource is used
     * for scheduling. Default value is 1.
     * This is different from spark.executor.cores, which defines the task parallelism
     * inside a stage.
     */
    public static final String RAY_ACTOR_CPU_RESOURCE = RAY_ACTOR_RESOURCE_PREFIX + ".cpu";

    public static final int DEFAULT_SPARK_CORES_PER_EXECUTOR = 1;
}
