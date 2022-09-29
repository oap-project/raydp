package com.intel.raydp.shims.spark321

import com.intel.raydp.shims.{SparkShims, SparkShimDescriptor}

object SparkShimProvider {
  val SPARK311_DESCRIPTOR = SparkShimDescriptor(3, 1, 1)
  val SPARK312_DESCRIPTOR = SparkShimDescriptor(3, 1, 2)
  val SPARK313_DESCRIPTOR = SparkShimDescriptor(3, 1, 3)
  val SPARK321_DESCRIPTOR = SparkShimDescriptor(3, 2, 1)
  val DESCRIPTOR_STRINGS =
    Seq(s"$SPARK311_DESCRIPTOR", s"$SPARK312_DESCRIPTOR",
        s"$SPARK313_DESCRIPTOR", s"$SPARK321_DESCRIPTOR",)
}

class SparkShimProvider extends com.intel.raydp.shims.SparkShimProvider {
  def createShim: SparkShims = {
    new Spark321Shims()
  }

  def matches(version: String): Boolean = {
    SparkShimProvider.DESCRIPTOR_STRINGS.contains(version)
  }
}
