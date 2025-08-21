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

package org.apache.spark.deploy.raydp

import java.io.File
import java.text.SimpleDateFormat
import java.util.{Date, Locale}
import javax.xml.bind.DatatypeConverter

import scala.collection.mutable.HashMap
import scala.jdk.CollectionConverters._

import com.fasterxml.jackson.core.JsonFactory
import com.fasterxml.jackson.databind.ObjectMapper

import io.ray.api.{ActorHandle, PlacementGroups, Ray}
import io.ray.api.id.PlacementGroupId
import io.ray.api.placementgroup.PlacementGroup
import io.ray.runtime.config.RayConfig

import org.apache.spark.{RayDPException, SecurityManager, SparkConf}
import org.apache.spark.executor.RayDPExecutor
import org.apache.spark.internal.Logging
import org.apache.spark.raydp.{RayExecutorUtils, SparkOnRayConfigs}
import org.apache.spark.rpc._
import org.apache.spark.util.Utils


class RayAppMaster(host: String,
                   port: Int,
                   actorExtraClasspath: String) extends Serializable with Logging {
  private var endpoint: RpcEndpointRef = _
  private var rpcEnv: RpcEnv = _
  private val conf: SparkConf = new SparkConf()
  private val restartedExecutors = new HashMap[String, String]()

  init()

  def this() = {
    this(RayConfig.create().nodeIp, 0, "")
  }

  def this(actorExtraClasspath: String) = {
    this(RayConfig.create().nodeIp, 0, actorExtraClasspath)
  }

  def init(): Unit = {
    Utils.loadDefaultSparkProperties(conf)
    val securityMgr = new SecurityManager(conf)
    rpcEnv = RpcEnv.create(
      RayAppMaster.ENV_NAME,
      host,
      host,
      port,
      conf,
      securityMgr,
      numUsableCores = 0,
      clientMode = false)
    // register endpoint
    endpoint = rpcEnv.setupEndpoint(RayAppMaster.ENDPOINT_NAME, new RayAppMasterEndpoint(rpcEnv))
  }

  /**
   * Get the app master endpoint URL. The executor will connect to AppMaster by this URL and
   * tell the AppMaster that it has started up successful.
   */
  def getAppMasterEndpointUrl(): String = {
    RpcEndpointAddress(rpcEnv.address, RayAppMaster.ENDPOINT_NAME).toString
  }

  def getRestartedExecutors(): java.util.Map[String, String] = restartedExecutors.asJava

  /**
   * This is used to represent the Spark on Ray cluster URL.
   */
  def getMasterUrl(): String = {
    val url = RpcEndpointAddress(rpcEnv.address, RayAppMaster.ENDPOINT_NAME).toString
    url.replace("spark", "ray")
  }

  def stop(): Int = {
    logInfo("Stopping RayAppMaster")
    if (rpcEnv != null) {
      rpcEnv.shutdown()
      endpoint = null
      rpcEnv = null
    }
    0
  }

  class RayAppMasterEndpoint(override val rpcEnv: RpcEnv)
      extends ThreadSafeRpcEndpoint with Logging {
    // For application IDs
    private def createDateFormat = new SimpleDateFormat("yyyyMMddHHmmssSSS", Locale.US)
    private var driverEndpoint: RpcEndpointRef = null
    private var driverAddress: RpcAddress = null
    private var appInfo: ApplicationInfo = null
    private var nodesWithShuffleService
                  = new HashMap[String, ActorHandle[RayExternalShuffleService]]()

    private val shuffleServiceOptions = RayExternalShuffleService.getShuffleConf(conf)

    private val placementGroup: PlacementGroup = conf
      .getOption("spark.ray.placement_group")
      .map { hex =>
        val id = PlacementGroupId.fromBytes(DatatypeConverter.parseHexBinary(hex))
        PlacementGroups.getPlacementGroup(id)
      }.orNull
    private val bundleIndexesOpt: Option[Array[Int]] = conf.getOption("spark.ray.bundle_indexes")
      .map(_.split(",").map(_.toInt))

    private val bundleIndexesNum: Int = bundleIndexesOpt match {
        case Some(n) => n.size
        case None => 0
      }

    private var currentBundleIndex: Int = 0

    override def receive: PartialFunction[Any, Unit] = {
      case RegisterApplication(appDescription: ApplicationDescription, driver: RpcEndpointRef) =>

        logInfo("Registering app " + appDescription.name)
        val javaOpts = appendActorClasspath(appDescription.command.javaOpts)
        val newCommand = appDescription.command.withNewJavaOpts(javaOpts)
        val updatedAppDesc = appDescription.withNewCommand(newCommand)
        val app = createApplication(updatedAppDesc, driver)
        registerApplication(app)
        logInfo("Registered app " + appDescription.name + " with ID " + app.id)
        driver.send(RegisteredApplication(app.id, self))
        schedule()

      case UnregisterApplication(appId) =>
        assert(appInfo != null && appInfo.id == appId)
        appInfo.markFinished(ApplicationState.FINISHED)
    }

    override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
      case RegisterExecutor(executorId, executorIp) =>
        val success = appInfo.registerExecutor(executorId)
        if (success) {
          // external shuffle service is enabled
          if (conf.getBoolean("spark.shuffle.service.enabled", false)) {
            // the node executor is in has not started shuffle service
            if (!nodesWithShuffleService.contains(executorIp)) {
              logInfo(s"Starting shuffle service on ${executorIp}")
              val service = ExternalShuffleServiceUtils.createShuffleService(
                executorIp, shuffleServiceOptions.toBuffer.asJava)
              ExternalShuffleServiceUtils.startShuffleService(service)
              nodesWithShuffleService(executorIp) = service
            }
          }
          setUpExecutor(executorId)
        }
        context.reply(success)

      case ExecutorStarted(executorId) =>
        appInfo.markExecutorStarted(executorId, context.senderAddress)
        context.reply(true)

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
          if (!appInfo.kill(executorId, shutdownActor = true)) {
            success = false
          }
        }
        context.reply(success)

      case RequestAddPendingRestartedExecutor(executorId) =>
        if (appInfo.remainingUnRegisteredExecutors > 0) {
          val cores = appInfo.desc.coresPerExecutor.getOrElse(1)
          val memory = appInfo.desc.memoryPerExecutorMB
          // ray actor will restart using the old ID
          val handlerOpt = Ray.getActor("raydp-executor-" + executorId)
          if (!handlerOpt.isPresent) {
            context.reply(AddPendingRestartedExecutorReply(None))
          } else {
            val newExecutorId = s"${appInfo.getNextExecutorId()}"
            val handler = handlerOpt.get.asInstanceOf[ActorHandle[RayDPExecutor]]
            appInfo.addPendingRegisterExecutor(newExecutorId, handler, cores, memory)
            restartedExecutors(newExecutorId) = executorId
            context.reply(AddPendingRestartedExecutorReply(Some(newExecutorId)))
          }
        } else {
          context.reply(AddPendingRestartedExecutorReply(None))
        }
    }

    override def onDisconnected(remoteAddress: RpcAddress): Unit = {
      appInfo.kill(remoteAddress, shutdownActor = false)
    }

    override def onStop(): Unit = {
      if (nodesWithShuffleService != null) {
        nodesWithShuffleService.values.foreach(handle =>
            ExternalShuffleServiceUtils.stopShuffleService(handle)
        )
        nodesWithShuffleService = null
      }
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
      val appId = "app-%s".format(createDateFormat.format(submitDate))
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
      val sparkCoresPerExecutor = appInfo.desc
        .coresPerExecutor
        .getOrElse(SparkOnRayConfigs.DEFAULT_SPARK_CORES_PER_EXECUTOR)
      val rayActorCPU = this.appInfo.desc.rayActorCPU
      val memory = appInfo.desc.memoryPerExecutorMB
      val executorId = s"${appInfo.getNextExecutorId()}"

      logInfo(
        s"Requesting Spark executor with Ray logical resource " +
          s"{ CPU: $rayActorCPU, " +
          s"${appInfo.desc.resourceReqsPerExecutor
            .map { case (name, amount) => s"$name: $amount" }.mkString(", ")} }..")
      // TODO: Support generic fractional logical resources using prefix spark.ray.actor.resource.*

      // This will check with dynamic auto scale no additional pending executor actor added more
      // than max executors count as this result in executor even running after job completion
      val dynamicAllocationEnabled = conf.getBoolean("spark.dynamicAllocation.enabled", false)
      // FIX: Check total executors (current + restarted) against maxExecutor, executorInstances
      if (dynamicAllocationEnabled) {
        val maxExecutor = conf.getInt("spark.dynamicAllocation.maxExecutors", 0)
        if ((appInfo.executors.size + restartedExecutors.size) >= maxExecutor) {
          return
        }
      } else {
        val executorInstances = conf.getInt("spark.executor.instances", 0)
        if (executorInstances != 0 &&
          (appInfo.executors.size + restartedExecutors.size) >= executorInstances) {
          return
        }
      }

      val handler = RayExecutorUtils.createExecutorActor(
        executorId,
        getAppMasterEndpointUrl(),
        rayActorCPU,
        memory,
        // This won't work, Spark expect integer in custom resources,
        // please see python test test_spark_on_fractional_custom_resource
        appInfo.desc.resourceReqsPerExecutor
          .map { case (name, amount) => (name, Double.box(amount)) }.asJava,
        placementGroup,
        getNextBundleIndex,
        appInfo.desc.command.javaOpts.asJava)
      appInfo.addPendingRegisterExecutor(executorId, handler, sparkCoresPerExecutor, memory)
    }

    private def appendActorClasspath(javaOpts: Seq[String]): Seq[String] = {
      var user_set_cp = false
      var i = 0
      while (!user_set_cp && i < javaOpts.size) {
        if ("-cp" == javaOpts(i) || "-classpath" == javaOpts(i)) {
          user_set_cp = true
        }
        i += 1
      }

      if (user_set_cp) {
        // user has set '-cp' or '-classpath'
        if (i == javaOpts.size) {
          throw new RayDPException(
            s"Found ${javaOpts(i - 1)} but classpath url not presented in executor java opts")
        }

        javaOpts.updated(i, javaOpts(i) + File.pathSeparator + actorExtraClasspath)
      } else {
        // user has not set, we append the actor extra classpath in the end
        javaOpts ++ Seq("-cp", actorExtraClasspath)
      }
    }

    private def setUpExecutor(executorId: String): Unit = {
      val handlerOpt = appInfo.getExecutorHandler(executorId)
      if (handlerOpt.isEmpty) {
        logWarning(s"Trying to setup executor: ${executorId} which has been removed")
      }
      val driverUrl = appInfo.desc.command.driverUrl
      val cores = appInfo.desc.coresPerExecutor.getOrElse(1)
      val appId = appInfo.id
      val classPathEntries = appInfo.desc.command.classPathEntries.mkString(";")
      RayExecutorUtils.setUpExecutor(handlerOpt.get, appId, driverUrl, cores, classPathEntries)
    }

    private def getNextBundleIndex: Int = {
      if (placementGroup != null && bundleIndexesNum != 0) {
        val previous = currentBundleIndex
        currentBundleIndex = (currentBundleIndex + 1) % bundleIndexesNum
        previous
      } else {
        -1
      }
    }
  }
}

object RayAppMaster extends Serializable {
  val ENV_NAME = "RAY_RPC_ENV"
  val ENDPOINT_NAME = "RAY_APP_MASTER"
  val ACTOR_NAME = "RAY_APP_MASTER"

  def setProperties(properties: String): Unit = {
    // Use Jackson ObjectMapper directly to avoid JSON4S version conflicts
    val mapper = new ObjectMapper()
    val javaMap = mapper.readValue(properties, classOf[java.util.Map[String, Object]])
    val scalaMap = javaMap.asScala.toMap
    scalaMap.foreach{ case (key, value) =>
      // Convert all values to strings since System.setProperty expects String
      System.setProperty(key, value.toString)
    }

    // Use the same session dir as the python side
    RayConfig.create().setSessionDir(System.getProperty("ray.session-dir"))
  }

  def shutdownRay(): Unit = {
    Ray.shutdown()
  }
}
