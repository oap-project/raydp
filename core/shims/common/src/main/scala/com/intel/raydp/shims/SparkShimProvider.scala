package com.intel.raydp.shims

/**
 * Provider interface for matching and retrieving the Shims of a specific Spark version
 */
trait SparkShimProvider {
  def matches(version:String): Boolean
  def createShim: SparkShims
}
