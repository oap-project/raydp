package org.apache.spark.executor

import java.net.URL

import org.apache.spark.SparkEnv
import org.apache.spark.rpc.RpcEnv
import org.apache.spark.resource.ResourceProfile

trait RayDPExecutorBackendFactory {
  def createExecutorBackend(
      rpcEnv: RpcEnv,
      driverUrl: String,
      executorId: String,
      bindAddress: String,
      hostname: String,
      cores: Int,
      userClassPath: Seq[URL],
      env: SparkEnv,
      resourcesFileOpt: Option[String],
      resourceProfile: ResourceProfile): CoarseGrainedExecutorBackend
}
