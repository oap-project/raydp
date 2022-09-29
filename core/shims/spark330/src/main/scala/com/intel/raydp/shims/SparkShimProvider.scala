package com.intel.raydp.shims.spark330

import com.intel.raydp.shims.{SparkShims, SparkShimDescriptor}

object SparkShimProvider {
  val DESCRIPTOR = SparkShimDescriptor(3, 3, 0)
  val DESCRIPTOR_STRINGS = Seq(s"$DESCRIPTOR")
}

class SparkShimProvider extends com.intel.raydp.shims.SparkShimProvider {
  def createShim: SparkShims = {
    new Spark330Shims()
  }

  def matches(version: String): Boolean = {
    SparkShimProvider.DESCRIPTOR_STRINGS.contains(version)
  }
}
