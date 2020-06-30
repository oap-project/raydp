package org.apache.spark.scheduler.cluster.ray

import org.apache.spark.deploy.raydp.{ApplicationDescription, Command}
import org.apache.spark.internal.{Logging, config}
import org.apache.spark.launcher.{LauncherBackend, SparkAppHandle}
import org.apache.spark.resource.ResourceUtils
import org.apache.spark.rpc.RpcEndpointAddress
import org.apache.spark.{RayDPException, SparkConf, SparkContext}
import org.apache.spark.scheduler.TaskSchedulerImpl
import org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend
import org.apache.spark.util.Utils

class RayCoarseGrainedSchedulerBackend(
    sc: SparkContext,
    scheduler: TaskSchedulerImpl,
    masterURL: String)
  extends CoarseGrainedSchedulerBackend(scheduler, sc.env.rpcEnv) with Logging {

  private val launcherBackend = new LauncherBackend() {
    override protected def conf: SparkConf = sc.conf
    override protected def onStopRequest(): Unit = stop(SparkAppHandle.State.KILLED)
  }

  override def start(): Unit = {
    super.start()

    val conf = sc.conf

    if (sc.deployMode != "client") {
      throw new RayDPException("We only support client mode currently")
    }

    if (Utils.isDynamicAllocationEnabled(conf)) {
      throw new RayDPException("Dynamic Allocation is not supported currently")
    }

    launcherBackend.connect()

    val driverUrl = RpcEndpointAddress(
      conf.get(config.DRIVER_HOST_ADDRESS),
      conf.get(config.DRIVER_PORT),
      CoarseGrainedSchedulerBackend.ENDPOINT_NAME
    ).toString
    val args = Seq(
      "--driver-url", driverUrl,
      "--executor-id", "{{EXECUTOR_ID}}",
      "--hostname", "{{HOSTNAME}}",
      "--cores", "{{CORES}}",
      "--app-id", "{{APP_ID}}",
      "--worker-url", "{{WORKER_URL}}")
    val extraJavaOpts = sc.conf.get(config.EXECUTOR_JAVA_OPTIONS)
      .map(Utils.splitCommandString).getOrElse(Seq.empty)
    val classPathEntries = sc.conf.get(config.EXECUTOR_CLASS_PATH)
      .map(_.split(java.io.File.pathSeparator).toSeq).getOrElse(Nil)
    val libraryPathEntries = sc.conf.get(config.EXECUTOR_LIBRARY_PATH)
      .map(_.split(java.io.File.pathSeparator).toSeq).getOrElse(Nil)

    // When testing, expose the parent class path to the child. This is processed by
    // compute-classpath.{cmd,sh} and makes all needed jars available to child processes
    // when the assembly is built with the "*-provided" profiles enabled.
    val testingClassPath =
    if (sys.props.contains(config.Tests.IS_TESTING.key)) {
      sys.props("java.class.path").split(java.io.File.pathSeparator).toSeq
    } else {
      Nil
    }

    // Start executors with a few necessary configs for registering with the scheduler
    val sparkJavaOpts = Utils.sparkJavaOpts(conf, SparkConf.isExecutorStartupConf)
    val javaOpts = sparkJavaOpts ++ extraJavaOpts
    val command = Command(
      args, sc.executorEnvs, classPathEntries ++ testingClassPath, libraryPathEntries, javaOpts)
    val coresPerExecutor = conf.getOption(config.EXECUTOR_CORES.key).map(_.toInt)

    val executorResourceReqs = ResourceUtils.parseResourceRequirements(
      conf, config.SPARK_EXECUTOR_PREFIX)
    val numExecutors = conf.get(config.EXECUTOR_INSTANCES).get
    val appDesc = ApplicationDescription(sc.appName, numExecutors, coresPerExecutor,
      sc.executorMemory, command, resourceReqsPerExecutor = executorResourceReqs)

    launcherBackend.setState(SparkAppHandle.State.SUBMITTED)
    waitForRegistration()
    launcherBackend.setState(SparkAppHandle.State.RUNNING)
  }


  def waitForRegistration(): Unit = {}

}
