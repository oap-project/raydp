package org.apache.spark


class RayDPException(message: String, cause: Throwable) extends SparkException(message, cause){
  def this(message: String) = this(message, null)
}
