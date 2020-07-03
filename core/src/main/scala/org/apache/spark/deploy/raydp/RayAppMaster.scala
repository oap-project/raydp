package org.apache.spark.deploy.raydp

import java.text.SimpleDateFormat
import java.util.{Date, Locale}

import io.ray.api.function.RayFuncVoid5
import io.ray.runtime.config.RayConfig
import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.executor.RayCoarseGrainedExecutorBackend
import org.apache.spark.internal.Logging
import org.apache.spark.raydp.RayProxy
import org.apache.spark.rpc._

import scala.collection.JavaConverters._

class RayAppMaster(
    override val rpcEnv: RpcEnv,
    address: RpcAddress,
    val securityMgr: SecurityManager,
    val conf: SparkConf)
  extends ThreadSafeRpcEndpoint with Serializable with Logging {

  // register endpoint
  rpcEnv.setupEndpoint(RayAppMaster.ENDPOINT_NAME, this)

  // For application IDs
  private def createDateFormat = new SimpleDateFormat("yyyyMMddHHmmss", Locale.US)

  private val masterUrl =
    RpcEndpointAddress(address.host, address.port, RayAppMaster.ENDPOINT_NAME)

  private var driverEndpoint: RpcEndpointRef = null
  private var driverAddress: RpcAddress = null
  private var appInfo: ApplicationInfo = null

  private var nextAppNumber = 0

  override def receive: PartialFunction[Any, Unit] = {
    case RegisterApplication(appDescription: ApplicationDescription, driver: RpcEndpointRef) =>
      logInfo("Registering app " + appDescription.name)
      val app = createApplication(appDescription, driver)
      registerApplication(app)
      logInfo("Registered app " + appDescription.name + " with ID " + app.id)
      driver.send(RegisteredApplication(app.id, self))
      schedule()
  }


  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case RegisterExecutor(executorId) =>
      val success = appInfo.registerExecutor(executorId)
      if (success) {
        setUpExecutor(executorId)
      }
      context.reply(success)

    case RequestExecutors(appId, requestedTotal) =>
      assert(appInfo != null && appInfo.id == appId)
      if (requestedTotal > appInfo.currentExecutors()) {
        (0 until (requestedTotal - appInfo.currentExecutors())).foreach{ _ =>
          requestNewExecutor()
        }
      }
      context.reply(true)

    case KillExecutors(appId, executorIds) =>
      assert(appInfo != null && appInfo.id == appId)
      var success = true
      for (executorId <- executorIds) {
        if (!appInfo.kill(executorId)) {
          success = false
        }
      }
      context.reply(success)
  }

  private def createApplication(
      desc: ApplicationDescription, driver: RpcEndpointRef): ApplicationInfo = {
    val now = System.currentTimeMillis()
    val date = new Date(now)
    val appId = newApplicationId(date)
    new ApplicationInfo(now, appId, desc, date, driver)
  }

  /** Generate a new app ID given an app's submission date */
  private def newApplicationId(submitDate: Date): String = {
    val appId = "app-%s-%04d".format(createDateFormat.format(submitDate), nextAppNumber)
    nextAppNumber += 1
    appId
  }

  private def registerApplication(app: ApplicationInfo): Unit = {
    val appAddress = app.driver.address
    if (appAddress == driverAddress) {
      logInfo("Attempted to re-register application at same address: " + appAddress)
      return
    }

    appInfo = app
    driverEndpoint = app.driver
    driverAddress = appAddress
  }

  private def schedule(): Unit = {
    val desc = appInfo.desc
    for (_ <- 0 until desc.numExecutors) {
      requestNewExecutor()
    }
  }

  private def requestNewExecutor(): Unit = {
    val cores = appInfo.desc.coresPerExecutor.getOrElse(1)
    val memory = appInfo.desc.memoryPerExecutorMB
    val executorId = s"${appInfo.getNextExecutorId()}"
    val javaOpts = appInfo.desc.command.javaOpts.mkString(" ")
    val handler = RayProxy.createExecutorActor(
      executorId, masterUrl.toString, cores,
      memory,
      appInfo.desc.resourceReqsPerExecutor.map(pair => (pair._1, Double.box(pair._2))).asJava,
      javaOpts)
    appInfo.addPendingRegisterExecutor(executorId, handler, cores, memory)
  }

  private def setUpExecutor(executorId: String): Unit = {
    val handlerOpt = appInfo.getExecutorHandler(executorId)
    if (handlerOpt.isEmpty) {
      logWarning(s"Trying to setup executor: ${executorId} which has been removed")
    }
    val driverUrl = appInfo.desc.command.driverUrl
    val cores = appInfo.desc.coresPerExecutor
    val appId = appInfo.id
    val classPathEntries = appInfo.desc.command.classPathEntries
    val func = new RayFuncVoid5[RayCoarseGrainedExecutorBackend, String, String, Any, Seq[String]] {
      override def apply(
          obj: RayCoarseGrainedExecutorBackend,
          appId: String,
          driverUrl: String,
          cores: Any,
          classPathEntries: Seq[String]): Unit = {
        obj.startUp(appId, driverUrl, cores.asInstanceOf[Int], classPathEntries)
      }
    }

    handlerOpt.get.task(func, appId, driverUrl, cores, classPathEntries).remote()
  }
}


object RayAppMaster extends Serializable {
  val ENV_NAME = "RAY_RPC_ENV"
  val ENDPOINT_NAME = "RAY_APP_MASTER"

  def apply(): RayAppMaster = {
    val conf = new SparkConf()
    val securityMgr = new SecurityManager(conf)
    val nodeIp = RayConfig.getInstance().nodeIp
    val env = RpcEnv.create(
      RayAppMaster.ENV_NAME,
      nodeIp,
      nodeIp,
      -1,
      conf,
      securityMgr,
      numUsableCores = 0,
      clientMode = false)
    new RayAppMaster(env, env.address, securityMgr, conf)
  }
}
