package org.apache.spark.raydp;


public class SparkOnRayConfigs {
    @Deprecated
    public static final String RAY_ACTOR_RESOURCE_PREFIX = "spark.ray.actor.resource";

    public static final String SPARK_EXECUTOR_ACTOR_RESOURCE_PREFIX =
            "spark.ray.raydp_spark_executor.actor.resource";
    public static final String SPARK_MASTER_ACTOR_RESOURCE_PREFIX =
            "spark.ray.raydp_spark_master.actor.resource";

    /**
     * Extra JVM options for the RayDP AppMaster actor and gateway process.
     * This is useful for passing JDK 17+ --add-opens flags.
     * Example: "--add-opens=java.base/java.lang=ALL-UNNAMED ..."
     */
    public static final String SPARK_APP_MASTER_EXTRA_JAVA_OPTIONS =
            "spark.ray.raydp_app_master.extraJavaOptions";

    /**
     * CPU cores per Ray Actor which host the Spark executor, the resource is used
     * for scheduling. Default value is 1.
     * This is different from spark.executor.cores, which defines the task parallelism
     * inside a stage.
     */
    @Deprecated
    public static final String RAY_ACTOR_CPU_RESOURCE = RAY_ACTOR_RESOURCE_PREFIX + ".cpu";

    /**
     * CPU cores per Ray Actor which host the Spark executor, the resource is used
     * for scheduling. Default value is 1.
     * This is different from spark.executor.cores, which defines the task parallelism
     * inside a stage.
     */
    public static final String SPARK_EXECUTOR_ACTOR_CPU_RESOURCE =
            SPARK_EXECUTOR_ACTOR_RESOURCE_PREFIX + ".cpu";

    public static final int DEFAULT_SPARK_CORES_PER_EXECUTOR = 1;

    // For below log4j related configs, there are multiple JVM processes
    // involved, like
    // spark driver, AppMasterEntryPoint and ray worker (master and executor).
    // To make doc simple, group them into two parts, driver (spark driver) and
    // ray worker (AppMasterEntryPoint, master and executor), though
    // AppMasterEntryPoint is not inside ray worker

    /**
     * Set to path of Java agent jar. Prefixed with 'spark.' so that spark
     * doesn't filter it out. It's converted to JVM option
     * '-javaagent:[jar path]' before JVM gets started.
     *
     * It's used internally and applied to all JVM processes.
     */
    public static final String SPARK_JAVAAGENT = "spark.javaagent";

    /**
     * Set to correct log4j version based on actual spark version and ray
     * version. For driver, it's set to 'log4j' if spark version <= 3.2.
     * 'log4j2' otherwise. For ray worker, it's set to 'log4j2'.
     * {@link org.slf4j.impl.StaticLoggerBinder} then loads right
     * {@link org.slf4j.ILoggerFactory} based this config.
     *
     * It's used internally and applied to all JVM processes.
     */
    public static final String LOG4J_FACTORY_CLASS_KEY = "spark.ray.log4j.factory.class";

    /**
     * Log4j 1 and log4j 2 use different system property names,
     * 'log4j.configurationFile' and 'log4j2.configurationFile' to configure
     * which file to use. Since ray uses log4j2 as always, it's a constant
     * here.
     *
     * It's used internally for ray worker.
     */
    public static final String RAY_LOG4J_CONFIG_FILE_NAME = "log4j2.configurationFile";

    /**
     * Ray uses 'log4j2.xml' as its default log4j configuration file. User can
     * set to different file by setting this config in 'init_spark' function.
     *
     * The similar config 'spark.log4j.config.file.name' for spark driver is set
     * in python. See versions.py and ray_cluster_master.py.
     *
     * It's for user configiration and defaults to 'log4j2.xml'. It's for ray
     * worker only.
     */
    public static final String RAY_LOG4J_CONFIG_FILE_NAME_KEY = "spark.ray.log4j.config.file.name";

    /**
     * From python, we set some extra java options in spark config for spark
     * driver. However, this option should not be propogated to ray worker.
     * It gets excluded in
     * {@link org.apache.spark.deploy.raydp.AppMasterJavaBridge}.
     */
    public static final String SPARK_DRIVER_EXTRA_JAVA_OPTIONS = "spark.driver.extraJavaOptions";

    /**
     * User may want to use their own jar instead of one from spark.
     * User can set this config to their jars separated with ":" for spark
     * driver.
     *
     * It's for user configuration and defaults to empty. It's for spark driver
     * only.
     */
    public static final String SPARK_PREFER_CLASSPATH = "spark.preferClassPath";

    /**
     * Same as above configure, but for ray worker.
     *
     * It's for user configuration and defaults to empty. It's for ray worker
     * only.
     */
    public static final String RAY_PREFER_CLASSPATH = "spark.ray.preferClassPath";

    /**
     * The default log file prefix is 'java-worker' which is monitored and polled
     * by ray log monitor. As spark executor log, we don't want it's monitored and
     * polled since user doesn't care about this relative large amount of logs in most time.
     *
     * There is a PR, https://github.com/ray-project/ray/pull/33797, which enables
     * us to change default log file prefix and thus avoid being monitored and polled.
     *
     * This configure is to change the prefix to 'raydp-java-worker'.
     */
    public static final String RAYDP_LOGFILE_PREFIX_CFG =
            "-Dray.logging.file-prefix=raydp-java-worker";
}
