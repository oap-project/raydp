package org.apache.spark.deploy.raydp

import org.apache.spark.rpc.RpcEndpointRef

private[deploy] sealed trait RayDPDeployMessage extends Serializable

case class RegisterApplication(appDescription: ApplicationDescription, driver: RpcEndpointRef)
  extends RayDPDeployMessage

case class RegisteredApplication(appId: String, master: RpcEndpointRef) extends RayDPDeployMessage

case class RegisterExecutor(executorId: Int) extends RayDPDeployMessage