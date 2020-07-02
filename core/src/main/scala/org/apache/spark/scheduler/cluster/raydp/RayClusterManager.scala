package org.apache.spark.scheduler.cluster.raydp

import org.apache.spark.SparkContext
import org.apache.spark.scheduler.{ExternalClusterManager, SchedulerBackend, TaskScheduler, TaskSchedulerImpl}

private[spark] class RayClusterManager extends ExternalClusterManager {

  override def canCreate(masterURL: String): Boolean = {
    masterURL.startsWith("ray")
  }

  override def createTaskScheduler(sc: SparkContext, masterURL: String): TaskScheduler = {
    new TaskSchedulerImpl(sc)
  }

  override def createSchedulerBackend(
      sc: SparkContext,
      masterURL: String,
      scheduler: TaskScheduler): SchedulerBackend = {
    new RayCoarseGrainedSchedulerBackend(
      sc,
      scheduler.asInstanceOf[TaskSchedulerImpl],
      masterURL)
  }

  override def initialize(scheduler: TaskScheduler, backend: SchedulerBackend): Unit = {
    scheduler.asInstanceOf[TaskSchedulerImpl].initialize(backend)
  }
}
