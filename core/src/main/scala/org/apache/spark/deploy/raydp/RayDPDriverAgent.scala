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

import io.ray.runtime.config.RayConfig

import org.apache.spark.{SecurityManager, SparkContext, SparkConf}
import org.apache.spark.internal.Logging
import org.apache.spark.rpc._


class RayDPDriverAgent() {
  private val spark = SparkContext.getOrCreate()
  private var endpoint: RpcEndpointRef = _
  private var rpcEnv: RpcEnv = _
  private val conf: SparkConf = new SparkConf()

  init

  def init(): Unit = {
    val securityMgr = new SecurityManager(conf)
    val host = RayConfig.create().nodeIp
    rpcEnv = RpcEnv.create(
      RayAppMaster.ENV_NAME,
      host,
      host,
      0,
      conf,
      securityMgr,
      // limit to single-thread
      numUsableCores = 1,
      clientMode = false)
    // register endpoint
    endpoint = rpcEnv.setupEndpoint(RayDPDriverAgent.ENDPOINT_NAME,
      new RayDPDriverAgentEndpoint(rpcEnv))
  }

  def getDriverAgentEndpointUrl(): String = {
    RpcEndpointAddress(rpcEnv.address, RayDPDriverAgent.ENDPOINT_NAME).toString
  }

  class RayDPDriverAgentEndpoint(override val rpcEnv: RpcEnv)
      extends ThreadSafeRpcEndpoint with Logging {
    override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
      case RecacheRDD(rddId) =>
        // TODO if multiple blocks get lost, should call this only once
        // SparkEnv.get.blockManagerMaster.getLocationsAndStatus()
        spark.getPersistentRDDs.map {
          case (id, rdd) =>
            if (id == rddId) {
              rdd.count
            }
        }
        context.reply(true)
    }
  }

}

object RayDPDriverAgent {
  val ENDPOINT_NAME = "RAYDP_DRIVER_AGENT"
}