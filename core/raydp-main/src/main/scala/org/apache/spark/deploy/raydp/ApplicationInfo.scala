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

import java.util.Date

import scala.collection.mutable.{ArrayBuffer, HashMap}

import io.ray.api.ActorHandle

import org.apache.spark.executor.RayDPExecutor
import org.apache.spark.internal.Logging
import org.apache.spark.raydp.RayExecutorUtils
import org.apache.spark.resource.ResourceInformation
import org.apache.spark.rpc.{RpcAddress, RpcEndpointRef}


case class ExecutorDesc(
    executorId: String,
    cores: Int,
    memoryPerExecutorMB: Int,
    resources: Map[String, ResourceInformation]) {
  var registered: Boolean = false
}

private[spark] class ApplicationInfo(
    val startTime: Long,
    val id: String,
    val desc: ApplicationDescription,
    val submitDate: Date,
    val driver: RpcEndpointRef)
  extends Logging {

  var state: ApplicationState.Value = _
  var executors: HashMap[String, ExecutorDesc] = _
  var addressToExecutorId: HashMap[RpcAddress, String] = _
  var executorIdToHandler: HashMap[String, ActorHandle[RayDPExecutor]] = _
  var removedExecutors: ArrayBuffer[ExecutorDesc] = _
  var coresGranted: Int = _
  var endTime: Long = _
  private var nextExecutorId: Int = _
  // this only count those registered executors and minus removed executors
  private var registeredExecutors: Int = 0

  init()

  private def init(): Unit = {
    state = ApplicationState.WAITING
    executors = new HashMap[String, ExecutorDesc]
    addressToExecutorId = new HashMap[RpcAddress, String]
    executorIdToHandler = new HashMap[String, ActorHandle[RayDPExecutor]]
    endTime = -1L
    nextExecutorId = 0
    removedExecutors = new ArrayBuffer[ExecutorDesc]
  }

  def addPendingRegisterExecutor(
      executorId: String,
      handler: ActorHandle[RayDPExecutor],
      cores: Int,
      memoryInMB: Int): Unit = {
    val desc = ExecutorDesc(executorId, cores, memoryInMB, null)
    executors(executorId) = desc
    executorIdToHandler(executorId) = handler
  }

  def registerExecutor(executorId: String): Boolean = {
    if (executors.contains(executorId)) {
      if (executors(executorId).registered) {
        logWarning(s"Try to register executor: ${executorId} twice")
        false
      } else {
        executors(executorId).registered = true
        registeredExecutors += 1
        true
      }
    } else {
      logWarning(s"Try to register executor: ${executorId} which is not existed")
      false
    }
  }

  def markExecutorStarted(executorId: String, address: RpcAddress): Unit = {
    addressToExecutorId(address) = executorId
  }

  def kill(address: RpcAddress, shutdownActor: Boolean): Boolean = {
    if (addressToExecutorId.contains(address)) {
      kill(addressToExecutorId(address), shutdownActor)
    } else {
      false
    }
  }

  def kill(executorId: String, shutdownActor: Boolean): Boolean = {
    if (executors.contains(executorId)) {
      val exec = executors(executorId)
      if (exec.registered) {
        registeredExecutors -= 1
      }
      removedExecutors += executors(executorId)
      executors -= executorId
      coresGranted -= exec.cores
      if (shutdownActor) {
        // Previously we used to exitExecutor for all scenarios, but it will cause
        // the following issue when a executor is down because of OOM issue:
        // - Executor E1 dies at T0 lets say because of OOm
        // - We try to kill it by firing stop call on E1 actor
        // - Since the actor is not available, the stop task fails for E1
        // - In the mean while, ray brings up the lost executor E1
        // - The failed task (stop task) gets retried as there are task retries configured.
        // - The stop task gets fired on the new executor which got recovered
        // - The Recovered executor exits with status as user intended exit.
        RayExecutorUtils.exitExecutor(executorIdToHandler(executorId))
      }
      executorIdToHandler -= executorId
      true
    } else {
      false
    }
  }

  def getExecutorHandler(
      executorId: String): Option[ActorHandle[RayDPExecutor]] = {
    executorIdToHandler.get(executorId)
  }

  def remainingUnRegisteredExecutors(): Int = {
    desc.numExecutors - registeredExecutors
  }

  def currentExecutors(): Int = {
    registeredExecutors
  }

  def getNextExecutorId(): Int = {
    val previous = nextExecutorId
    nextExecutorId += 1
    previous
  }

  private var _retryCount: Int = 0

  def retryCount: Int = _retryCount

  def incrementRetryCount(): Int = {
    _retryCount += 1
    _retryCount
  }

  def resetRetryCount(): Unit = _retryCount = 0

  def markFinished(endState: ApplicationState.Value): Unit = {
    state = endState
    endTime = System.currentTimeMillis()
  }

  def isFinished: Boolean = {
    state != ApplicationState.WAITING && state != ApplicationState.RUNNING
  }

  def duration: Long = {
    if (endTime != -1) {
      endTime - startTime
    } else {
      System.currentTimeMillis() - startTime
    }
  }
}
