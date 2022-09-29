package com.intel.raydp.shims.spark330

import org.apache.spark.api.java.JavaRDD
import org.apache.spark.executor.RayDPExecutorBackendFactory
import org.apache.spark.executor.spark330._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.spark330.SparkSqlUtils

import com.intel.raydp.shims.{ShimDescriptor, SparkShims}

class Spark330Shims extends SparkShims {
  override def getShimDescriptor: ShimDescriptor = SparkShimProvider.DESCRIPTOR

  override def toDataFrame(
      rdd: JavaRDD[Array[Byte]],
      schema: String,
      session: SparkSession): DataFrame = {
    SparkSqlUtils.toDataFrame(rdd, schema, session)
  }

  override def getExecutorBackendFactory(): RayDPExecutorBackendFactory = {
    new RayDPSpark330ExecutorBackendFactory()
  }
}
