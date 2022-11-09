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

import org.apache.spark.rpc.RpcEndpointRef

private[deploy] sealed trait RayDPDeployMessage extends Serializable

case class RegisterApplication(appDescription: ApplicationDescription, driver: RpcEndpointRef)
  extends RayDPDeployMessage

case class RegisteredApplication(appId: String, master: RpcEndpointRef) extends RayDPDeployMessage

case class UnregisterApplication(appId: String) extends RayDPDeployMessage

case class RegisterExecutor(executorId: String, nodeIp: String) extends RayDPDeployMessage

case class ExecutorStarted(executorId: String) extends RayDPDeployMessage

case class RequestExecutors(appId: String, requestedTotal: Int) extends RayDPDeployMessage

case class KillExecutors(appId: String, executorIds: Seq[String]) extends RayDPDeployMessage

case class RequestAddPendingRestartedExecutor(executorId: String)
  extends RayDPDeployMessage

case class AddPendingRestartedExecutorReply(newExecutorId: Option[String])
  extends RayDPDeployMessage

case class RecacheRDD(rddId: Int) extends RayDPDeployMessage
