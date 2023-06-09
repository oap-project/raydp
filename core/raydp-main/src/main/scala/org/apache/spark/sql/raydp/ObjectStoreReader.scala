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
import java.nio.ByteBuffer
import java.nio.channels.{Channels, ReadableByteChannel}
import java.util.List

import scala.collection.JavaConverters._

import com.intel.raydp.shims.SparkShimLoader
import org.apache.arrow.vector.VectorSchemaRoot

import org.apache.spark.TaskContext
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.raydp.RayDPUtils
import org.apache.spark.rdd.{RayDatasetRDD, RayObjectRefRDD}
import org.apache.spark.sql.{DataFrame, SparkSession, SQLContext}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.execution.arrow.ArrowConverters
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch, ColumnVector}

object ObjectStoreReader {
  def createRayObjectRefDF(
      spark: SparkSession,
      locations: List[Array[Byte]]): DataFrame = {
    val rdd = new RayObjectRefRDD(spark.sparkContext, locations)
    val schema = new StructType().add("idx", IntegerType)
    spark.createDataFrame(rdd, schema)
  }

  def fromRootIterator(
      arrowRootIter: Iterator[VectorSchemaRoot],
      schema: StructType,
      timeZoneId: String): Iterator[InternalRow] = {
    val arrowSchema = ArrowUtils.toArrowSchema(schema, timeZoneId)

    new Iterator[InternalRow] {
      private var rowIter = if (arrowRootIter.hasNext) nextBatch() else Iterator.empty

      override def hasNext: Boolean = rowIter.hasNext || {
        if (arrowRootIter.hasNext) {
          rowIter = nextBatch()
          true
        } else {
          false
        }
      }

      override def next(): InternalRow = rowIter.next()

      private def nextBatch(): Iterator[InternalRow] = {
        val root = arrowRootIter.next()
        val columns = root.getFieldVectors.asScala.map { vector =>
          new ArrowColumnVector(vector).asInstanceOf[ColumnVector]
        }.toArray

        val batch = new ColumnarBatch(columns)
        batch.setNumRows(root.getRowCount)
        root.close()
        batch.rowIterator().asScala
      }
    }
  }

  def RayDatasetToDataFrame(
      sparkSession: SparkSession,
      rdd: RayDatasetRDD,
      schemaString: String): DataFrame = {
    val schema = DataType.fromJson(schemaString).asInstanceOf[StructType]
    val sqlContext = new SQLContext(sparkSession)
    val timeZoneId = sqlContext.sessionState.conf.sessionLocalTimeZone
    val resultRDD = JavaRDD.fromRDD(rdd).rdd.mapPartitions { it =>
      fromRootIterator(it, schema, timeZoneId)
    }
    sqlContext.internalCreateDataFrame(resultRDD.setName("arrow"), schema)
  }

  def getBatchesFromStream(
      ref: Array[Byte]): Iterator[VectorSchemaRoot] = {
    val objectRef = RayDPUtils.readBinary(ref, classOf[VectorSchemaRoot])
    Iterator[VectorSchemaRoot](objectRef.get)
  }
}
