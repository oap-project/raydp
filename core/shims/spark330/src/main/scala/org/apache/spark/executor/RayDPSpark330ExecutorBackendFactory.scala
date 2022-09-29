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