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

package org.apache.spark.scheduler.cluster.raydp

import java.net.URI
import java.util.concurrent.Semaphore
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}

import scala.collection.mutable.HashMap
import scala.concurrent.Future

import io.ray.api.{ActorHandle, Ray}

import org.apache.spark.{RayDPException, SparkConf, SparkContext}
import org.apache.spark.deploy.raydp._
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.launcher.{LauncherBackend, SparkAppHandle}
import org.apache.spark.resource.{ResourceRequirement, ResourceUtils}
import org.apache.spark.rpc.{RpcEndpointAddress, RpcEndpointRef, RpcEnv, ThreadSafeRpcEndpoint}
import org.apache.spark.scheduler.TaskSchedulerImpl
import org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend
import org.apache.spark.util.Utils

/**
 * A SchedulerBackend that request executor from Ray.
 */
class RayCoarseGrainedSchedulerBackend(
    sc: SparkContext,
    scheduler: TaskSchedulerImpl,
    masterURL: String)
  extends CoarseGrainedSchedulerBackend(scheduler, sc.env.rpcEnv) with Logging {

  private val masterSparkUrl = transferOrCreateRPCEndpoint(masterURL).toString

  private val appId = new AtomicReference[String]()
  private var masterHandle: ActorHandle[RayAppMaster] = _
  private val appMasterRef = new AtomicReference[RpcEndpointRef]()
  private val stopped = new AtomicBoolean()

  private val registrationBarrier = new Semaphore(0)

  private val launcherBackend = new LauncherBackend() {
    override protected def conf: SparkConf = sc.conf
    override protected def onStopRequest(): Unit = stop(SparkAppHandle.State.KILLED)
  }

  def transferOrCreateRPCEndpoint(sparkUrl: String): RpcEndpointAddress = {
    try {
      var uri: URI = null
      if (sparkUrl == "ray") {
        // not yet started
        Ray.init()
        val cp = sys.props("java.class.path")
        val options = RayExternalShuffleService.getShuffleConf(conf)
        masterHandle = RayAppMasterUtils.createAppMaster(cp, options)
        uri = new URI(RayAppMasterUtils.getMasterUrl(masterHandle))
      } else {
        uri = new URI(sparkUrl)
      }
      val host = uri.getHost
      val port = uri.getPort
      val name = uri.getUserInfo
      if (uri.getScheme != "ray" ||
        host == null ||
        port < 0 ||
        name == null ||
        (uri.getPath != null && !uri.getPath.isEmpty) || // uri.getPath returns "" instead of null
        uri.getFragment != null ||
        uri.getQuery != null) {
        throw new RayDPException("Invalid Ray Master URL: " + sparkUrl)
      }
      new RpcEndpointAddress(host, port, name)
    } catch {
      case e: java.net.URISyntaxException =>
        throw new RayDPException("Invalid Ray Master URL: " + sparkUrl, e)
    }
  }

  override def start(): Unit = {
    super.start()

    val conf = sc.conf

    if (sc.deployMode != "client") {
      throw new RayDPException("We only support client mode currently")
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
    // add Xmx, it should not be set in java opts, because Spark is not allowed.
    // We also add Xms to ensure the Xmx >= Xms
    val memoryLimit = Seq(s"-Xms${sc.executorMemory}M", s"-Xmx${sc.executorMemory}M")
    val javaOpts = sparkJavaOpts ++ extraJavaOpts ++ memoryLimit

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

  override def stop(): Unit = {
    stop(SparkAppHandle.State.FINISHED)
    if (masterHandle != null) {
      RayAppMasterUtils.stopAppMaster(masterHandle)
    }
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
        appMasterRef.set(ref)
        registrationBarrier.release()
    }

    def registerToAppMaster(): Unit = {
      logInfo("Registering to app master " + masterURL + "...")
      val appMasterRef = rpcEnv.setupEndpointRefByURI(masterSparkUrl)
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
  override def doKillExecutors(executorIds: Seq[String]): Future[Boolean] = {
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
        super.stop() // this will stop all executors
        appMasterRef.get.send(UnregisterApplication(appId.get))
      } finally {
        appMasterRef.set(null)
        launcherBackend.setState(finalState)
        launcherBackend.close()
      }
    }
  }
}
