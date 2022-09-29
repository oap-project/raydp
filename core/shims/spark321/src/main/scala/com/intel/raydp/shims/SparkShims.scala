package com.intel.raydp.shims.spark321

import org.apache.spark.api.java.JavaRDD
import org.apache.spark.executor.RayDPExecutorBackendFactory
import org.apache.spark.executor.spark321._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.spark321.SparkSqlUtils

import com.intel.raydp.shims.{ShimDescriptor, SparkShims}

class Spark321Shims extends SparkShims {
  override def getShimDescriptor: ShimDescriptor = SparkShimProvider.DESCRIPTOR

  override def toDataFrame(
      rdd: JavaRDD[Array[Byte]],
      schema: String,
      session: SparkSession): DataFrame = {
    SparkSqlUtils.toDataFrame(rdd, schema, session)
  }

  override def getExecutorBackendFactory(): RayDPExecutorBackendFactory = {
    new RayDPSpark321ExecutorBackendFactory()
  }
}
