package org.apache.spark.executor

import java.net.URL

import io.ray.runtime.config.RayConfig
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.deploy.raydp.RegisterExecutor
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.rpc.{RpcEndpointRef, RpcEnv}
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.{RetrieveSparkAppConfig, SparkAppConfig}
import org.apache.spark.util.Utils
import org.apache.spark.{SecurityManager, SparkConf, SparkEnv}


class RayCoarseGrainedExecutorBackend(
    val executorId: String,
    val masterURL: String) extends Logging {

  val nodeIp = RayConfig.getInstance().nodeIp

  private val temporaryRpcEnvName = "ExecutorTemporaryRpcEnv"
  private var temporaryRpcEnv: Option[RpcEnv] = None

  init()

  def init(): Unit = {
    val conf = new SparkConf()
    createTemporaryRpcEnv(temporaryRpcEnvName, conf)
    assert(temporaryRpcEnv.nonEmpty)
    registerToAppMaster()
  }

  def registerToAppMaster(): Unit = {
    var driver: RpcEndpointRef = null
    val nTries = 3
    for (i <- 0 until nTries if driver == null) {
      try {
        driver = temporaryRpcEnv.get.setupEndpointRefByURI(masterURL)
      } catch {
        case e: Throwable =>
          if (i == nTries - 1) {
            throw e
          } else {
            logWarning(
              s"Executor: ${executorId} register to app master failed(${i + 1}/${nTries}) ")
          }
      }
    }

    val registeredResult = driver.askSync[Boolean](RegisterExecutor(executorId))
    if (registeredResult) {
      logInfo(s"Executor: ${executorId} register to app master success")
    } else {
      throw new RuntimeException(s"Executor: ${executorId} register to app master failed")
    }
  }

  def startUp(
      appId: String, driverUrl: String, cores: Int, classPathEntries: Seq[String]): Unit = {
    val userClassPath = classPathEntries.map(new URL(_))
    val createFn: (RpcEnv, SparkEnv, ResourceProfile) =>
      CoarseGrainedExecutorBackend = {
      case (rpcEnv, env, resourceProfile) =>
        new CoarseGrainedExecutorBackend(rpcEnv, driverUrl, executorId,
          nodeIp, nodeIp, cores, userClassPath, env, None, resourceProfile)
    }
    run(appId, driverUrl, cores, createFn)
    System.exit(0)
  }

  private def run(
      appId: String,
      driverUrl: String,
      cores: Int,
      backendCreateFn: (RpcEnv, SparkEnv, ResourceProfile) => CoarseGrainedExecutorBackend
  ): Unit = {

    Utils.initDaemon(log)

    SparkHadoopUtil.get.runAsSparkUser { () =>
      var driver: RpcEndpointRef = null
      val nTries = 3
      for (i <- 0 until nTries if driver == null) {
        try {
          driver = temporaryRpcEnv.get.setupEndpointRefByURI(driverUrl)
        } catch {
          case e: Throwable => if (i == nTries - 1) {
            throw e
          }
        }
      }

      val cfg = driver.askSync[SparkAppConfig](
        RetrieveSparkAppConfig(ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID))
      val props = cfg.sparkProperties ++ Seq[(String, String)](("spark.app.id", appId))
      destroyTemporaryRpcEnv()

      // Create SparkEnv using properties we fetched from the driver.
      val driverConf = new SparkConf()
      for ((key, value) <- props) {
        // this is required for SSL in standalone mode
        if (SparkConf.isExecutorStartupConf(key)) {
          driverConf.setIfMissing(key, value)
        } else {
          driverConf.set(key, value)
        }
      }

      cfg.hadoopDelegationCreds.foreach { tokens =>
        SparkHadoopUtil.get.addDelegationTokens(tokens, driverConf)
      }

      driverConf.set(EXECUTOR_ID, executorId)
      val env = SparkEnv.createExecutorEnv(driverConf, executorId, nodeIp,
        nodeIp, cores, cfg.ioEncryptionKey, isLocal = false)

      env.rpcEnv.setupEndpoint("Executor", backendCreateFn(env.rpcEnv, env, cfg.resourceProfile))

      env.rpcEnv.awaitTermination()
    }
  }

  def createTemporaryRpcEnv(
      name: String,
      conf: SparkConf): Unit = {
    val env = RpcEnv.create(
      name,
      nodeIp,
      nodeIp,
      -1,
      conf,
      new SecurityManager(conf),
      numUsableCores = 0,
      clientMode = true)
    temporaryRpcEnv = Some(env)
  }

  def destroyTemporaryRpcEnv(): Unit = {
    if (temporaryRpcEnv.nonEmpty) {
      temporaryRpcEnv.get.shutdown()
      temporaryRpcEnv = None
    }
  }
}
