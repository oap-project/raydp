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

package org.apache.spark.sql.raydp

import java.io.ByteArrayInputStream
import java.nio.channels.{Channels, ReadableByteChannel}
import java.util.List

import com.intel.raydp.shims.SparkShimLoader

import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}
import org.apache.spark.raydp.RayDPUtils
import org.apache.spark.rdd.{RayDatasetRDD, RayObjectRefRDD}
import org.apache.spark.sql.{DataFrame, SparkSession, SQLContext}
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.execution.arrow.ArrowConverters
import org.apache.spark.sql.types.{StringType, StructType}

object ObjectStoreReader {
  def createRayObjectRefDF(
      spark: SparkSession,
      blocksHex: List[String],
      locations: List[Array[Byte]]): DataFrame = {
    val rdd = new RayObjectRefRDD(spark.sparkContext, blocksHex, locations)
    val schema = new StructType().add("hex", StringType)
    spark.createDataFrame(rdd, schema)
  }

  def RayDatasetToDataFrame(
      sparkSession: SparkSession,
      rdd: RayDatasetRDD,
      schema: String): DataFrame = {
    SparkShimLoader.getSparkShims.toDataFrame(JavaRDD.fromRDD(rdd), schema, sparkSession)
  }

  def getBatchesFromStream(
      ref: Array[Byte]): Iterator[Array[Byte]] = {
    val objectRef = RayDPUtils.readBinary(ref, classOf[Array[Byte]])
    ArrowConverters.getBatchesFromStream(
        Channels.newChannel(new ByteArrayInputStream(objectRef.get)))
  }
}
