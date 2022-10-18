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

package org.apache.spark.executor.spark330

import java.net.URL

import org.apache.spark.SparkEnv
import org.apache.spark.executor._
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.rpc.RpcEnv

class RayDPSpark330ExecutorBackendFactory
    extends RayDPExecutorBackendFactory {
  override def createExecutorBackend(
      rpcEnv: RpcEnv,
      driverUrl: String,
      executorId: String,
      bindAddress: String,
      hostname: String,
      cores: Int,
      userClassPath: Seq[URL],
      env: SparkEnv,
      resourcesFileOpt: Option[String],
      resourceProfile: ResourceProfile): CoarseGrainedExecutorBackend = {
    new RayCoarseGrainedExecutorBackend(
      rpcEnv,
      driverUrl,
      executorId,
      bindAddress,
      hostname,
      cores,
      userClassPath,
      env,
      resourcesFileOpt,
      resourceProfile)
  }
}
