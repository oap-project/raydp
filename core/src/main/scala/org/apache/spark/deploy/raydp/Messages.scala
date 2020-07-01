package org.apache.spark.deploy.raydp

import org.apache.spark.rpc.RpcEndpointRef

private[deploy] sealed trait RayDPDeployMessage extends Serializable

case class RegisterApplication(appDescription: ApplicationDescription, driver: RpcEndpointRef)
  extends RayDPDeployMessage

case class RegisteredApplication(appId: String, master: RpcEndpointRef) extends RayDPDeployMessage

case class RegisterExecutor(executorId: String) extends RayDPDeployMessage

case class RequestExecutors(appId: String, requestedTotal: Int) extends RayDPDeployMessage

case class KillExecutors(appId: String, executorIds: Seq[String]) extends RayDPDeployMessage
