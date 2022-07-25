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

import scala.collection.Map

private[spark] case class Command(
    driverUrl: String,
    environment: Map[String, String],
    classPathEntries: Seq[String],
    libraryPathEntries: Seq[String],
    javaOpts: Seq[String]) {

  def withNewJavaOpts(newJavaOptions: Seq[String]): Command = {
    Command(driverUrl, environment, classPathEntries, libraryPathEntries, newJavaOptions)
  }
}

private[spark] case class ApplicationDescription(
    name: String,
    numExecutors: Int,
    coresPerExecutor: Option[Int],
    memoryPerExecutorMB: Int,
    rayActorCPU: Double,
    command: Command,
    user: String = System.getProperty("user.name", "<unknown>"),
    resourceReqsPerExecutor: Map[String, Double] = Map.empty,
    rayActorGPU: Double = 0d) {

  def withNewCommand(newCommand: Command): ApplicationDescription = {
    ApplicationDescription(name = name, numExecutors = numExecutors, coresPerExecutor = coresPerExecutor,
      memoryPerExecutorMB = memoryPerExecutorMB, command = newCommand, user = user,
      resourceReqsPerExecutor = resourceReqsPerExecutor, rayActorCPU = rayActorCPU, rayActorGPU = rayActorGPU)
  }
}
