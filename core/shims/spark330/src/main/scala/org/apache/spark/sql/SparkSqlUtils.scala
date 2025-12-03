/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.spark330

import org.apache.arrow.vector.types.pojo.Schema
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.sql.execution.arrow.ArrowConverters
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.ArrowUtils

object SparkSqlUtils {
  def toDataFrame(rdd: JavaRDD[Array[Byte]], schema: String, session: SparkSession): DataFrame = {
    ArrowConverters.toDataFrame(rdd, schema, session)
  }

  def toArrowSchema(schema : StructType, timeZoneId : String, sparkSession: SparkSession) : Schema = {
    ArrowUtils.toArrowSchema(schema = schema, timeZoneId = timeZoneId)
  }

  def toArrowRDD(dataFrame: DataFrame, sparkSession: SparkSession): RDD[Array[Byte]] = {
    dataFrame.toArrowBatchRdd
  }
}
