package org.apache.spark.executor.spark321

import java.net.URL

import org.apache.spark.SparkEnv
import org.apache.spark.executor.CoarseGrainedExecutorBackend
import org.apache.spark.executor.RayDPExecutorBackendFactory
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.rpc.RpcEnv

class RayDPSpark321ExecutorBackendFactory
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
    new CoarseGrainedExecutorBackend(
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