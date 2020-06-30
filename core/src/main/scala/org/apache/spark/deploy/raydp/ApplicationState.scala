package org.apache.spark.deploy.raydp

private[master] object ApplicationState extends Enumeration {

  type ApplicationState = Value

  val WAITING, RUNNING, FINISHED, FAILED, KILLED, UNKNOWN = Value
}
