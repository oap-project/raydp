/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.executor

import java.io.File
import java.nio.file.Paths
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
import org.apache.spark.{RayDPException, SecurityManager, SparkConf, SparkEnv}

import org.apache.log4j.{FileAppender => Log4jFileAppender, _}

class RayCoarseGrainedExecutorBackend(
    val executorId: String,
    val appMasterURL: String) extends Logging {

  val nodeIp = RayConfig.getInstance().nodeIp

  private val temporaryRpcEnvName = "ExecutorTemporaryRpcEnv"
  private var temporaryRpcEnv: Option[RpcEnv] = None
  private var executorRunningThread: Thread = null
  private var workingDir: File = null

  init()

  def init(): Unit = {
    val conf = new SparkConf()
    createTemporaryRpcEnv(temporaryRpcEnvName, conf)
    assert(temporaryRpcEnv.nonEmpty)
    registerToAppMaster()
  }

  def registerToAppMaster(): Unit = {
    var appMaster: RpcEndpointRef = null
    val nTries = 3
    for (i <- 0 until nTries if appMaster == null) {
      try {
        appMaster = temporaryRpcEnv.get.setupEndpointRefByURI(appMasterURL)
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

    val registeredResult = appMaster.askSync[Boolean](RegisterExecutor(executorId))
    if (registeredResult) {
      logInfo(s"Executor: ${executorId} register to app master success")
    } else {
      throw new RuntimeException(s"Executor: ${executorId} register to app master failed")
    }
  }

  def startUp(
      appId: String,
      driverUrl: String,
      cores: Int,
      classPathEntries: String): Unit = {
    createWorkingDir(appId)
    setUserDir()
    redirectLog()

    val userClassPath = classPathEntries.split(";").filter(_.nonEmpty).map(new URL(_))
    val createFn: (RpcEnv, SparkEnv, ResourceProfile) =>
      CoarseGrainedExecutorBackend = {
      case (rpcEnv, env, resourceProfile) =>
        new CoarseGrainedExecutorBackend(rpcEnv, driverUrl, executorId,
          nodeIp, nodeIp, cores, userClassPath, env, None, resourceProfile)
    }
    executorRunningThread = new Thread() {
      override def run(): Unit = {
        try{
          serveAsExecutor(appId, driverUrl, cores, createFn)
        } catch {
          case e: Exception =>
            logWarning(e.getMessage)
            throw e
        }
      }
    }
    executorRunningThread.start()
  }

  def createWorkingDir(appId: String): Unit = {
    // create the application dir
    val app_dir = new File(RayConfig.getInstance().sessionDir, appId)
    var remainingTimes = 3
    var continue = true
    while (continue && remainingTimes > 0) {
      try {
        app_dir.mkdir()
        continue = !app_dir.exists()
        remainingTimes -= 1
        if (remainingTimes > 0) {
          logError(s"Create application dir: ${app_dir.getAbsolutePath} failed, " +
            s"remaining times: ${remainingTimes}")
        }
      } catch {
        case e: SecurityException =>
          throw e
      }
    }

    if (app_dir.exists()) {
      if (app_dir.isFile) {
        throw new RayDPException(
          s"Expect ${app_dir.getAbsolutePath} is a directory, however it is a file")
      }
    } else {
      throw new RayDPException(s"Create application dir: ${app_dir.getAbsolutePath} failed " +
        s"after 3 times trying")
    }

    val executor_dir = new File(app_dir.getCanonicalPath, executorId)
    if (executor_dir.exists()) {
      throw new RayDPException(
        s"Create ${executorId} working dir: ${executor_dir.getAbsolutePath} failed because " +
          s"it existed already")
    }
    executor_dir.mkdir()
    if (!executor_dir.exists()) {
      throw new RayDPException(s"Create ${executorId} working dir: " +
        s"${executor_dir.getAbsolutePath} failed")
    }

    workingDir = executor_dir.getCanonicalFile
  }

  def setUserDir(): Unit = {
    assert(workingDir != null && workingDir.isDirectory)
    System.setProperty("user.dir", workingDir.getAbsolutePath)
    System.setProperty("java.io.tmpdir", workingDir.getAbsolutePath)
    logInfo(s"Set user.dir to ${workingDir.getAbsolutePath}")
  }

  private def serveAsExecutor(
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

      // set the tmp dir for the executor, it will be deleted when executor stop
      val workerTmpDir = new File(workingDir, "_tmp")
      workerTmpDir.mkdir()
      assert(workerTmpDir.exists() && workerTmpDir.isDirectory)
      SparkEnv.get.driverTmpDir = Some(workerTmpDir.getAbsolutePath)

      env.rpcEnv.setupEndpoint("Executor", backendCreateFn(env.rpcEnv, env, cfg.resourceProfile))

      env.rpcEnv.awaitTermination()
    }
  }

  def redirectLog(): Unit = {
    val logFile = Paths.get(workingDir.getAbsolutePath, s"executor${executorId}.out")
    val errorFile = Paths.get(workingDir.getAbsolutePath, s"executor${executorId}.err")
    logInfo(s"Redirect executor log to ${logFile.toString}")
    val appenders = LogManager.getRootLogger.getAllAppenders
    // There should be a console appender. Use its layout.
    val defaultAppender = appenders.nextElement().asInstanceOf[Appender]
    val layout = defaultAppender.getLayout

    val out = new Log4jFileAppender(layout, logFile.toString)
    out.setName("outfile")

    val err = new Log4jFileAppender(layout, errorFile.toString())
    err.setName("errfile")
    err.setThreshold(Level.ERROR)

    LogManager.getRootLogger.addAppender(out)
    LogManager.getRootLogger.addAppender(err)
    LogManager.getRootLogger.removeAppender(defaultAppender)
  }

  def createTemporaryRpcEnv(
      name: String,
      conf: SparkConf): Unit = {
    val env = RpcEnv.create(name, nodeIp, nodeIp, -1, conf, new SecurityManager(conf),
      numUsableCores = 0, clientMode = true)
    temporaryRpcEnv = Some(env)
  }

  def destroyTemporaryRpcEnv(): Unit = {
    if (temporaryRpcEnv.nonEmpty) {
      temporaryRpcEnv.get.shutdown()
      temporaryRpcEnv = None
    }
  }
}
