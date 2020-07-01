package org.apache.spark.executor

import org.apache.spark.{SecurityManager, SparkConf, SparkEnv}
import org.apache.spark.executor.CoarseGrainedExecutorBackend.Arguments
import org.apache.spark.internal.Logging
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.rpc.{RpcEndpoint, RpcEndpointRef, RpcEnv}
import io.ray.api.Ray
import io.ray.runtime.config.RayConfig
import org.apache.spark.deploy.raydp.RegisterExecutor


class RayCoarseGrainedExecutorBackend(
    val executorId: String,
    val masterURL: String) extends Logging {

  init()

  def init(): Unit = {
    registerToAppMaster()
  }

  def registerToAppMaster(): Unit = {
    val ip = RayConfig.getInstance().nodeIp
    val conf = new SparkConf()
    val register = RpcEnv.create(
      "executorRegister",
      ip,
      ip,
      -1,
      conf,
      new SecurityManager(conf),
      numUsableCores = 0,
      clientMode = true)

    var driver: RpcEndpointRef = null
    val nTries = 3
    for (i <- 0 until nTries if driver == null) {
      try {
        driver = register.setupEndpointRefByURI(masterURL)
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
    register.shutdown()
  }


  def startUp(appId: String, driverUrl: String, cores: Int): Unit = {
    val createFn: (RpcEnv, Arguments, SparkEnv, ResourceProfile) =>
      CoarseGrainedExecutorBackend = {
      case (rpcEnv, arguments, env, resourceProfile) =>
        new CoarseGrainedExecutorBackend(rpcEnv, arguments.driverUrl, arguments.executorId,
          arguments.bindAddress, arguments.hostname, arguments.cores, arguments.userClassPath, env,
          arguments.resourcesFileOpt, resourceProfile)
    }
    val arguments = CoarseGrainedExecutorBackend.parseArguments(
      args, this.getClass.getCanonicalName.stripSuffix("$"))
    CoarseGrainedExecutorBackend.run(arguments, createFn)
    System.exit(0)
  }
}
