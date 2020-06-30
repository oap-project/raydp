package org.apache.spark.deploy.raydp

import java.text.SimpleDateFormat
import java.util.{Date, Locale}

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.raydp.RayProxy
import org.apache.spark.rpc.{RpcAddress, RpcCallContext, RpcEndpointAddress, RpcEndpointRef, RpcEnv, ThreadSafeRpcEndpoint}

import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}

private[deploy] class RayAppMaster(
    override val rpcEnv: RpcEnv,
    address: RpcAddress,
    val securityMgr: SecurityManager,
    val conf: SparkConf)
  extends ThreadSafeRpcEndpoint with Logging{

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
      context.reply(appInfo.registerExecutor(executorId))
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

  private def requestNewExecutor(appInfo: ApplicationInfo): Unit = {
    val driver = appInfo.driver
    val driverUrl = RpcEndpointAddress(driver.address, driver.name).toString
    val cores = appInfo.desc.coresPerExecutor
    val appId = appInfo.id



  }
}


object RayAppMaster {
  val ENDPOINT_NAME = "raydpAppMaster"

}
