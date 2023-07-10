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

import java.io.{ByteArrayOutputStream, File}
import java.nio.channels.Channels
import java.nio.file.Paths
import java.util.concurrent.atomic.AtomicBoolean

import scala.reflect.classTag

import com.intel.raydp.shims.SparkShimLoader
import io.ray.api.Ray
import io.ray.runtime.config.RayConfig
import org.apache.arrow.vector.ipc.{ArrowStreamWriter, WriteChannel}
import org.apache.arrow.vector.ipc.message.{IpcOption, MessageSerializer}
import org.apache.arrow.vector.types.pojo.Schema

import org.apache.spark._
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.deploy.raydp._
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.rpc.{RpcEndpointRef, RpcEnv}
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.{RetrieveSparkAppConfig, SparkAppConfig}
import org.apache.spark.storage.{BlockId, BlockManager}
import org.apache.spark.util.Utils

class RayDPExecutor(
    var executorId: String,
    val appMasterURL: String) extends Logging {

  val nodeIp = RayConfig.create().nodeIp
  val conf = new SparkConf()

  private val temporaryRpcEnvName = "ExecutorTemporaryRpcEnv"
  private var temporaryRpcEnv: Option[RpcEnv] = None
  private var executorRunningThread: Thread = null
  private var workingDir: File = null
  private val started = new AtomicBoolean(false)

  init()

  def init(): Unit = {
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
    // Check if this actor is restarted
    val ifRestarted = Ray.getRuntimeContext.wasCurrentActorRestarted
    if (ifRestarted) {
      val reply = appMaster.askSync[AddPendingRestartedExecutorReply](
          RequestAddPendingRestartedExecutor(executorId))
      // this executor might be restarted, use the returned new id and register self
      if (!reply.newExecutorId.isEmpty) {
        logInfo(s"Executor: ${executorId} seems to be restarted, registering using new id")
        executorId = reply.newExecutorId.get
      } else {
        throw new RuntimeException(s"Executor ${executorId} restarted, but getActor failed.")
      }
    }
    val registeredResult = appMaster.askSync[Boolean](RegisterExecutor(executorId, nodeIp))
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
    if (started.get) {
      throw new RayDPException("executor is already started")
    }
    createWorkingDir(appId)
    setUserDir()

    val userClassPath = classPathEntries.split(java.io.File.pathSeparator)
      .filter(_.nonEmpty).map(new File(_).toURI.toURL)
    val createFn: (RpcEnv, SparkEnv, ResourceProfile) =>
      CoarseGrainedExecutorBackend = {
      case (rpcEnv, env, resourceProfile) =>
        SparkShimLoader.getSparkShims
                       .getExecutorBackendFactory
                       .createExecutorBackend(rpcEnv, driverUrl, executorId,
          nodeIp, nodeIp, cores, userClassPath, env, None, resourceProfile)
    }
    executorRunningThread = new Thread() {
      override def run(): Unit = {
        try {
          serveAsExecutor(appId, driverUrl, cores, createFn)
        } catch {
          case e: Exception =>
            logWarning(e.getMessage)
            throw e
        }
      }
    }
    executorRunningThread.start()
    started.compareAndSet(false, true)
  }

  def createWorkingDir(appId: String): Unit = {
    // create the application dir
    val app_dir = new File(RayConfig.create().sessionDir, appId)
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

      val appMasterRef = env.rpcEnv.setupEndpointRefByURI(appMasterURL)
      appMasterRef.ask(ExecutorStarted(executorId))

      env.rpcEnv.setupEndpoint("Executor", backendCreateFn(env.rpcEnv, env, cfg.resourceProfile))

      env.rpcEnv.awaitTermination()
    }
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

  def stop(): Unit = {
    Ray.exitActor
  }

  def getBlockLocations(rddId: Int, numPartitions: Int): Array[String] = {
    val env = SparkEnv.get
    val blockIds = (0 until numPartitions).map(i =>
      BlockId.apply("rdd_" + rddId + "_" + i)
    ).toArray
    val locations = BlockManager.blockIdsToLocations(blockIds, env)
    var result = new Array[String](numPartitions)
    for ((key, value) <- locations) {
      val partitionId = key.name.substring(key.name.lastIndexOf('_') + 1).toInt
      result(partitionId) = value(0).substring(value(0).lastIndexOf('_') + 1)
    }
    result
  }

  def requestRecacheRDD(rddId: Int, driverAgentUrl: String): Unit = {
    val env = RpcEnv.create("TEMP_EXECUTOR_" + executorId, nodeIp, nodeIp, -1, conf,
                            new SecurityManager(conf),
                            numUsableCores = 0, clientMode = true)
    var driverAgent: RpcEndpointRef = null
    val nTries = 3
    for (i <- 0 until nTries if driverAgent == null) {
      try {
        driverAgent = env.setupEndpointRefByURI(driverAgentUrl)
      } catch {
        case e: Throwable =>
          if (i == nTries - 1) {
            throw e
          } else {
            logWarning(
              s"Executor: ${executorId} register to driver Agent failed(${i + 1}/${nTries}) ")
          }
      }
    }
    val success = driverAgent.askSync[Boolean](RecacheRDD(rddId))
    env.shutdown
  }

  def getRDDPartition(
      rddId: Int,
      partitionId: Int,
      schemaStr: String,
      driverAgentUrl: String): Array[Byte] = {
    while (!started.get) {
      // wait until executor is started
      // this might happen if executor restarts
      // and this task get retried
      try {
        Thread.sleep(1000)
      } catch {
        case e: Exception =>
          throw e
      }
    }
    val env = SparkEnv.get
    val context = SparkShimLoader.getSparkShims.getDummyTaskContext(partitionId, env)
    TaskContext.setTaskContext(context)
    val schema = Schema.fromJSON(schemaStr)
    val blockId = BlockId.apply("rdd_" + rddId + "_" + partitionId)
    val iterator = env.blockManager.get(blockId)(classTag[Array[Byte]]) match {
      case Some(blockResult) =>
        blockResult.data.asInstanceOf[Iterator[Array[Byte]]]
      case None =>
        logWarning("The cached block has been lost. Cache it again via driver agent")
        requestRecacheRDD(rddId, driverAgentUrl)
        env.blockManager.get(blockId)(classTag[Array[Byte]]) match {
          case Some(blockResult) =>
            blockResult.data.asInstanceOf[Iterator[Array[Byte]]]
          case None =>
            throw new RayDPException("Still cannot get the block after recache!")
        }
    }
    val byteOut = new ByteArrayOutputStream()
    val writeChannel = new WriteChannel(Channels.newChannel(byteOut))
    MessageSerializer.serialize(writeChannel, schema)
    iterator.foreach(writeChannel.write)
    ArrowStreamWriter.writeEndOfStream(writeChannel, new IpcOption)
    val result = byteOut.toByteArray
    writeChannel.close
    byteOut.close
    result
  }
}
