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
    val file = new File(RayConfig.getInstance().sessionDir, appId)
    var remainingTimes = 3
    var continue = true
    while (continue && remainingTimes > 0) {
      try {
        file.mkdir()
        continue = !file.exists()
        remainingTimes -= 1
        if (remainingTimes > 0) {
          logError(s"Create ${file.getAbsolutePath} failed, remaining times: ${remainingTimes}")
        }
      } catch {
        case e: SecurityException =>
          throw e
      }
    }

    if (file.exists()) {
      if (file.isFile) {
        throw new RayDPException(
          s"Expect ${file.getAbsolutePath} is a directory, however it is a file")
      }
    } else {
      throw new RayDPException(s"Create ${file.getAbsolutePath} failed after 3 times trying")
    }

    workingDir = file.getCanonicalFile
  }

  def setUserDir(): Unit = {
    assert(workingDir != null && workingDir.isDirectory)
    val file = new File(workingDir, executorId)
    if (file.exists()) {
      throw new RayDPException(
        s"Create ${executorId} working dir failed because it existed already")
    }
    try {
      file.mkdir()
    } catch {
      case e: Exception => throw e
    }

    if (!file.exists()) {
      throw new RayDPException(s"Create ${executorId} working dir failed")
    }
    System.setProperty("user.dir", file.getAbsolutePath)
    System.setProperty("java.io.tmpdir", file.getAbsolutePath)
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

      env.rpcEnv.setupEndpoint("Executor", backendCreateFn(env.rpcEnv, env, cfg.resourceProfile))

      env.rpcEnv.awaitTermination()
    }
  }

  def redirectLog(): Unit = {
    val logFile = Paths.get(workingDir.getAbsolutePath, executorId, 
      s"executor${executorId}.out")
    val errorFile = Paths.get(workingDir.getAbsolutePath, executorId, 
      s"executor${executorId}.err")
    logInfo(s"Redirect executor log to ${logFile.toString()}")
    val appenders = LogManager.getRootLogger().getAllAppenders()
    // There should be a console appender. Use its layout.
    val defaultAppender = appenders.nextElement().asInstanceOf[Appender]
    val layout = defaultAppender.getLayout()

    val out = new Log4jFileAppender(layout, logFile.toString())
    out.setName("outfile")
    
    val err = new Log4jFileAppender(layout, errorFile.toString())
    err.setName("errfile")
    err.setThreshold(Level.ERROR)

    LogManager.getRootLogger().addAppender(out)
    LogManager.getRootLogger().addAppender(err)
    LogManager.getRootLogger().removeAppender(defaultAppender)
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
