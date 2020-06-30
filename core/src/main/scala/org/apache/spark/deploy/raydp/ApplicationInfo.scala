package org.apache.spark.deploy.raydp

import java.util.Date

import io.ray.api.ActorHandle
import org.apache.spark.executor.RayCoarseGrainedExecutorBackend
import org.apache.spark.internal.Logging
import org.apache.spark.resource.ResourceInformation
import org.apache.spark.rpc.RpcEndpointRef

import scala.collection.mutable.{ArrayBuffer, HashMap}

case class ExecutorDesc(
    executorId: Int,
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
  var executors: HashMap[Int, ExecutorDesc] = _
  var executorIdToHandler: HashMap[Int, ActorHandle[RayCoarseGrainedExecutorBackend]] = _
  var removedExecutors: ArrayBuffer[ExecutorDesc] = _
  var coresGranted: Int = _
  var endTime: Long = _
  private var nextExecutorId: Int = _
  private var registeredExecutors: Int = 0

  init()

  private def init(): Unit = {
    state = ApplicationState.WAITING
    executors = new HashMap[Int, ExecutorDesc]
    executorIdToHandler = new HashMap[Int, ActorHandle[RayCoarseGrainedExecutorBackend]]
    endTime = -1L
    nextExecutorId = 0
    removedExecutors = new ArrayBuffer[ExecutorDesc]
  }

  def addPendingRegisterExecutor(
      handler: ActorHandle[RayCoarseGrainedExecutorBackend],
      cores: Int,
      memoryInMB: Int,
      resources: Map[String, ResourceInformation]): Unit = {
    val executorId = nextExecutorId
    nextExecutorId += 1
    val desc = ExecutorDesc(executorId, cores, memoryInMB, resources)
    executors(executorId) = desc
    executorIdToHandler(executorId) = handler
  }

  def registerExecutor(executorId: Int): Boolean = {
    if (executors.contains(executorId)) {
      if (executors(executorId).registered) {
        logWarning(s"Try to register executor: ${executorId} twice")
      } else {
        executors(executorId).registered = true
        registeredExecutors += 1
      }
      true
    } else {
      logWarning(s"Try to register executor: ${executorId} which is not existed")
      false
    }
  }

  def removeExecutor(executorId: Int): Unit = {
    if (executors.contains(executorId)) {
      val exec = executors(executorId)
      registeredExecutors -= 1
      removedExecutors += executors(executorId)
      executors -= executorId
      coresGranted -= exec.cores
      executorIdToHandler(executorId).kill(true) // kill the executor
      executorIdToHandler -= executorId
    }
  }

  private[master] def remainingUnRegisteredExecutors(): Int = {
    desc.numExecutors - registeredExecutors
  }

  private var _retryCount = 0

  private[master] def retryCount = _retryCount

  private[master] def incrementRetryCount() = {
    _retryCount += 1
    _retryCount
  }

  private[master] def resetRetryCount() = _retryCount = 0

  private[master] def markFinished(endState: ApplicationState.Value): Unit = {
    state = endState
    endTime = System.currentTimeMillis()
  }

  private[master] def isFinished: Boolean = {
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
