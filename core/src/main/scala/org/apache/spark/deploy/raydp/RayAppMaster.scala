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

import io.ray.runtime.config.RayConfig
import org.apache.spark.internal.Logging
import org.apache.spark.raydp.AppMasterJavaUtils
import org.apache.spark.rpc._
import org.apache.spark.deploy.ExternalShuffleService
import org.apache.spark.{RayDPException, SecurityManager, SparkConf}

import scala.collection.JavaConverters._

class RayAppMaster(host: String,
                   port: Int,
                   actor_extra_classpath: String) extends Serializable with Logging {
  private var endpoint: RpcEndpointRef = _
  private var rpcEnv: RpcEnv = _

  init()

  def this() = {
    this(RayConfig.create().nodeIp, 0, "")
  }

  def this(actor_extra_classpath: String) = {
    this(RayConfig.create().nodeIp, 0, actor_extra_classpath)
  }

  def init(): Unit = {
    val conf = new SparkConf()
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

  /**
   * This is used to represent the Spark on Ray cluster URL.
   */
  def getMasterUrl(): String = {
    val url = RpcEndpointAddress(rpcEnv.address, RayAppMaster.ENDPOINT_NAME).toString
    url.replace("spark", "ray")
  }

  def stop(): Unit = {
    if (rpcEnv != null) {
      rpcEnv.shutdown()
      endpoint = null
      rpcEnv = null
    }
  }

  class RayAppMasterEndpoint(override val rpcEnv: RpcEnv) extends ThreadSafeRpcEndpoint {
    // For application IDs
    private def createDateFormat = new SimpleDateFormat("yyyyMMddHHmmss", Locale.US)
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

      case UnregisterApplication(appId) =>
        assert(appInfo != null && appInfo.id == appId)
        appInfo.markFinished(ApplicationState.FINISHED)
    }

    override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
      case RegisterExecutor(executorId) =>
        val success = appInfo.registerExecutor(executorId)
        if (success) {
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
          if (!appInfo.kill(executorId)) {
            success = false
          }
        }
        context.reply(success)
    }

    override def onDisconnected(remoteAddress: RpcAddress): Unit = {
      appInfo.kill(remoteAddress)
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
      val javaOpts = appendActorClasspath(appInfo.desc.command.javaOpts)
      val handler = AppMasterJavaUtils.createExecutorActor(
        executorId, getAppMasterEndpointUrl(), cores,
        memory,
        appInfo.desc.resourceReqsPerExecutor.map(pair => (pair._1, Double.box(pair._2))).asJava,
        javaOpts)
      appInfo.addPendingRegisterExecutor(executorId, handler, cores, memory)
    }

    private def appendActorClasspath(javaOpts: Seq[String]): String = {
      var user_set_cp = false
      var i = 0
      while (user_set_cp && i < javaOpts.size) {
        if ("-cp" == javaOpts(i) || "-classpath" == javaOpts(i)) {
          user_set_cp = true
        }
      }

      val result = if (user_set_cp) {
        // user has set '-cp' or '-classpath'
        i += 1
        if (i == javaOpts.size) {
          throw new RayDPException(
            s"Found ${javaOpts(i-1)} while not classpath url in executor java opts")
        }

        javaOpts.updated(i, javaOpts(i) + File.pathSeparator + actor_extra_classpath)
      } else {
        // user has not set, we append the actor extra classpath in the end
        javaOpts ++ Seq("-cp", actor_extra_classpath)
      }
      result.mkString(" ")
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
      AppMasterJavaUtils.setUpExecutor(handlerOpt.get, appId, driverUrl, cores, classPathEntries)
    }
  }
}

object RayAppMaster extends Serializable {
  val ENV_NAME = "RAY_RPC_ENV"
  val ENDPOINT_NAME = "RAY_APP_MASTER"
}
