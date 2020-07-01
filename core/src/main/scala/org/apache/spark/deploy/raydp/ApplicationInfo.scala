package org.apache.spark.deploy.raydp

import java.util.Date

import io.ray.api.ActorHandle
import org.apache.spark.executor.RayCoarseGrainedExecutorBackend
import org.apache.spark.internal.Logging
import org.apache.spark.resource.ResourceInformation
import org.apache.spark.rpc.RpcEndpointRef

import scala.collection.mutable.{ArrayBuffer, HashMap}

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
  var executorIdToHandler: HashMap[String, ActorHandle[RayCoarseGrainedExecutorBackend]] = _
  var removedExecutors: ArrayBuffer[ExecutorDesc] = _
  var coresGranted: Int = _
  var endTime: Long = _
  private var nextExecutorId: Int = _
  // this only count those registered executors and minus removed executors
  private var registeredExecutors: Int = 0
  // this only count pending registered and minus
  private var totalRequestedExecutors: Int = 0

  init()

  private def init(): Unit = {
    state = ApplicationState.WAITING
    executors = new HashMap[String, ExecutorDesc]
    executorIdToHandler = new HashMap[String, ActorHandle[RayCoarseGrainedExecutorBackend]]
    endTime = -1L
    nextExecutorId = 0
    removedExecutors = new ArrayBuffer[ExecutorDesc]
  }

  def addPendingRegisterExecutor(
      executorId: String,
      handler: ActorHandle[RayCoarseGrainedExecutorBackend],
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

  def kill(executorId: String): Boolean = {
    if (executors.contains(executorId)) {
      val exec = executors(executorId)
      registeredExecutors -= 1
      removedExecutors += executors(executorId)
      executors -= executorId
      coresGranted -= exec.cores
      executorIdToHandler(executorId).kill(true) // kill the executor
      executorIdToHandler -= executorId
      true
    } else {
      false
    }
  }

  def getExecutorHandler(
      executorId: String): Option[ActorHandle[RayCoarseGrainedExecutorBackend]] = {
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

  private var _retryCount = 0

  def retryCount = _retryCount

  def incrementRetryCount() = {
    _retryCount += 1
    _retryCount
  }

  def resetRetryCount() = _retryCount = 0

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
