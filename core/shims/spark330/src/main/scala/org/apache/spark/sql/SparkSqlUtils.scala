package org.apache.spark.sql.spark330

import org.apache.spark.api.java.JavaRDD
import org.apache.spark.sql.{DataFrame, SparkSession, SQLContext}
import org.apache.spark.sql.execution.arrow.ArrowConverters

object SparkSqlUtils {
  def toDataFrame(rdd: JavaRDD[Array[Byte]], schema: String, session: SparkSession): DataFrame = {
    ArrowConverters.toDataFrame(rdd, schema, session)
  }
}
