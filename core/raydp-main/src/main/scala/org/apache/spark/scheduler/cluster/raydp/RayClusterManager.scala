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
