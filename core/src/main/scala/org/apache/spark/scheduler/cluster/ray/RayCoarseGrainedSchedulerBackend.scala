package org.apache.spark.scheduler.cluster.ray

import java.util.concurrent.Semaphore
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}

import org.apache.spark.deploy.raydp.{ApplicationDescription, Command, KillExecutors, RegisterApplication, RegisteredApplication, RequestExecutors}
import org.apache.spark.internal.{Logging, config}
import org.apache.spark.launcher.{LauncherBackend, SparkAppHandle}
import org.apache.spark.resource.{ResourceRequirement, ResourceUtils}
import org.apache.spark.rpc.{RpcEndpointAddress, RpcEndpointRef, RpcEnv, ThreadSafeRpcEndpoint}
import org.apache.spark.{RayDPException, SparkConf, SparkContext}
import org.apache.spark.scheduler.TaskSchedulerImpl
import org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend
import org.apache.spark.util.Utils

import scala.collection.mutable.HashMap
import scala.concurrent.Future

class RayCoarseGrainedSchedulerBackend(
    sc: SparkContext,
    scheduler: TaskSchedulerImpl,
    masterURL: String)
  extends CoarseGrainedSchedulerBackend(scheduler, sc.env.rpcEnv) with Logging {

  private val appId = new AtomicReference[String]()
  private val appMasterRef = new AtomicReference[RpcEndpointRef]()
  private val stopped = new AtomicBoolean()

  private val registrationBarrier = new Semaphore(0)

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
    val command = Command(driverUrl, sc.executorEnvs,
      classPathEntries ++ testingClassPath, libraryPathEntries, javaOpts)
    val coresPerExecutor = conf.getOption(config.EXECUTOR_CORES.key).map(_.toInt)

    val executorResourceReqs = ResourceUtils.parseResourceRequirements(
      conf, config.SPARK_EXECUTOR_PREFIX)
    val resourcesInMap = transferResourceRequirements(executorResourceReqs)
    val numExecutors = conf.get(config.EXECUTOR_INSTANCES).get
    val appDesc = ApplicationDescription(sc.appName, numExecutors, coresPerExecutor,
      sc.executorMemory, command, resourceReqsPerExecutor = resourcesInMap)
    val rpcEnv = sc.env.rpcEnv
    appMasterRef.set(rpcEnv.setupEndpoint(
      "AppMasterClient",
      new AppMasterClient(appDesc, rpcEnv)))
    launcherBackend.setState(SparkAppHandle.State.SUBMITTED)
    waitForRegistration()
    launcherBackend.setState(SparkAppHandle.State.RUNNING)
  }

  private def transferResourceRequirements(
      requirements: Seq[ResourceRequirement]): HashMap[String, Double] = {
    val results = HashMap[String, Double]()
    requirements.foreach{r =>
      val value = 1.0 * r.amount / r.numParts
      if (results.contains(r.resourceName)) {
        results(r.resourceName) = results(r.resourceName) + value
      } else {
        results += ((r.resourceName, value))
      }
    }
    results
  }

  def waitForRegistration(): Unit = {
    registrationBarrier.acquire()
  }

  private class AppMasterClient(
      appDesc: ApplicationDescription,
      override val rpcEnv: RpcEnv) extends ThreadSafeRpcEndpoint with Logging {
    private var appMasterRef: Option[RpcEndpointRef] = None

    override def onStart(): Unit = {
      try {
        registerToAppMaster()
      } catch {
        case e: Exception =>
          logWarning("Failed to connect to app master", e)
          stop()
      }
    }

    override def receive: PartialFunction[Any, Unit] = {
      case RegisteredApplication(id, ref) =>
        appId.set(id)
        launcherBackend.setAppId(id)
        appMasterRef = Some(ref)
        registrationBarrier.release()
    }

    def registerToAppMaster(): Unit = {
      logInfo("Connecting to app master " + masterURL + "...")
      val appMasterRef = rpcEnv.setupEndpointRefByURI(masterURL)
      appMasterRef.send(RegisterApplication(appDesc, self))
    }
  }

  /**
   * Request executors from the Master by specifying the total number desired,
   * including existing pending and running executors.
   *
   * @return whether the request is acknowledged.
   */
  override protected def doRequestTotalExecutors(requestedTotal: Int): Future[Boolean] = {
    if (appMasterRef.get != null && appId.get != null) {
      appMasterRef.get.ask[Boolean](RequestExecutors(appId.get, requestedTotal))
    } else {
      logWarning("Attempted to request executors before driver fully initialized.")
      Future.successful(false)
    }
  }

  /**
   * Kill the given list of executors through the Master.
   * @return whether the kill request is acknowledged.
   */
  def killExecutors(executorIds: Seq[String]): Future[Boolean] = {
    if (appMasterRef.get != null && appId.get != null) {
      appMasterRef.get.ask[Boolean](KillExecutors(appId.get, executorIds))
    } else {
      logWarning("Attempted to kill executors before driver fully initialized.")
      Future.successful(false)
    }
  }

   private def stop(finalState: SparkAppHandle.State): Unit = {
    if (stopped.compareAndSet(false, true)) {
      try {
        super.stop()
        appMasterRef.set(null)
      } finally {
        launcherBackend.setState(finalState)
        launcherBackend.close()
      }
    }
  }
}
