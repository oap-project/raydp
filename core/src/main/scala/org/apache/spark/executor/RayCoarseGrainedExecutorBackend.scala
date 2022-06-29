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

import scala.reflect.{classTag, ClassTag}

import java.io.{ByteArrayOutputStream, File}
import java.net.URL
import java.util.Properties
import java.nio.file.Paths
import java.nio.channels.{Channels, ReadableByteChannel}

import io.ray.runtime.config.RayConfig
import org.apache.log4j.{FileAppender => Log4jFileAppender, _}
import org.apache.arrow.vector.ipc.message.{IpcOption, MessageSerializer}
import org.apache.arrow.vector.ipc.{ArrowStreamWriter, WriteChannel}

import org.apache.spark.memory.TaskMemoryManager
import org.apache.spark.TaskContextImpl
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.arrow.vector.ipc.ArrowStreamWriter
import org.apache.spark.{Partition, RayDPException, SecurityManager, SparkConf, SparkContext, SparkEnv, TaskContext}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.deploy.raydp.{ExecutorStarted, RegisterExecutor}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.rdd.RDD;
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.rpc.{RpcEndpointRef, RpcEnv}
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.{RetrieveSparkAppConfig, SparkAppConfig}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.arrow.ArrowWriter
import org.apache.spark.sql.execution.columnar.DefaultCachedBatch
import org.apache.spark.sql.Row
import org.apache.spark.sql.raydp.ObjectStoreWriter
import org.apache.spark.storage.{BlockId, BlockManager, RDDBlockId, StorageLevel}
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.util.Utils
import org.apache.spark.sql.execution.python.BatchIterator


class RayCoarseGrainedExecutorBackend(
    val executorId: String,
    val appMasterURL: String) extends Logging {

  val nodeIp = RayConfig.create().nodeIp

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
    createWorkingDir(appId)
    setUserDir()
    // redirectLog()

    val userClassPath = classPathEntries.split(java.io.File.pathSeparator)
      .filter(_.nonEmpty).map(new File(_).toURI.toURL)
    val createFn: (RpcEnv, SparkEnv, ResourceProfile) =>
      CoarseGrainedExecutorBackend = {
      case (rpcEnv, env, resourceProfile) =>
        new CoarseGrainedExecutorBackend(rpcEnv, driverUrl, executorId,
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

  def getBlockLocations(rddId: Int, numPartitions: Int): Array[String] = {
    val env = SparkEnv.get
    val blockIds = (0 until numPartitions).map(i => 
      BlockId.apply("rdd_" + rddId + "_" + i)
    ).toArray
    val locations = BlockManager.blockIdsToLocations(blockIds, env)
    var result = new Array[String](numPartitions)
    for ((key, value) <- locations) {
      val name = key.name
      result(name.substring(name.lastIndexOf('_') + 1).toInt) = value(0)
    }
    result
  }

  def getRDDPartition(rdd: RDD[Array[Byte]], partition: Partition, schemaStr: String): Array[Byte] = {
    val env = SparkEnv.get
    val context = new TaskContextImpl(0, 0, partition.index, 0, 0,
        new TaskMemoryManager(env.memoryManager, 0), new Properties(), env.metricsSystem)
    TaskContext.setTaskContext(context)
  //   val rddIter = rdd.iterator(partition, context)
  //   val allocator = ObjectStoreWriter.getArrowAllocator(s"${rdd.id}_${partition.index}")
    val schema = Schema.fromJSON(schemaStr)
  //   val root = VectorSchemaRoot.create(schema, allocator)
    val byteOut = new ByteArrayOutputStream()
    val writeChannel = new WriteChannel(Channels.newChannel(byteOut))
    MessageSerializer.serialize(writeChannel, schema)
    rdd.iterator(partition, context).foreach(writeChannel.write)
    ArrowStreamWriter.writeEndOfStream(writeChannel, new IpcOption)
    byteOut.toByteArray
  //   val arrowWriter = ArrowWriter.create(root)
  //   var results: Array[Byte] = null
  //   // val accessor = InternalRow.getAccessor(IntegerType)
  //   val dataIter = new BatchIterator(rddIter, 50)
    
  //   Utils.tryWithSafeFinally {
  //     // write out the schema meta data
  //     val writer = new ArrowStreamWriter(root, null, byteOut)
  //     val iter = dataIter.next
  //     writer.start()
  //     while (iter.hasNext) {
  //       val row = iter.next
  //       // println(accessor(row, 0))
  //       arrowWriter.write(row.asInstanceOf[InternalRow])
  //     }

  //     // set the write record count
  //     arrowWriter.finish()
  //     // write out the record batch to the underlying out
  //     writer.writeBatch()

  //     // end writes footer to the output stream and doesn't clean any resources.
  //     // It could throw exception if the output stream is closed, so it should be
  //     // in the try block.
  //     writer.end()
  //     // get the wrote ByteArray and save to Ray ObjectStore
  //     results = byteOut.toByteArray
  //     byteOut.close()
  //   } {
  //     // If we close root and allocator in TaskCompletionListener, there could be a race
  //     // condition where the writer thread keeps writing to the VectorSchemaRoot while
  //     // it's being closed by the TaskCompletion listener.
  //     // Closing root and allocator here is cleaner because root and allocator is owned
  //     // by the writer thread and is only visible to the writer thread.
  //     //
  //     // If the writer thread is interrupted by TaskCompletionListener, it should either
  //     // (1) in the try block, in which case it will get an InterruptedException when
  //     // performing io, and goes into the finally block or (2) in the finally block,
  //     // in which case it will ignore the interruption and close the resources.

  //     root.close()
  //     allocator.close()
  //   }
  //   results
  }
}
