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

package org.apache.spark.rdd

import java.util.List;

import scala.collection.JavaConverters._

import io.ray.runtime.generated.Common.Address

import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.raydp.RayDPUtils
import org.apache.spark.sql.raydp.ObjectStoreReader

private[spark] class RayDatasetRDDPartition(val ref: Array[Byte], idx: Int) extends Partition {
  val index = idx
}

private[spark]
class RayDatasetRDD(
    jsc: JavaSparkContext,
    @transient val objectIds: List[Array[Byte]],
    locations: List[Array[Byte]])
  extends RDD[Array[Byte]](jsc.sc, Nil) {

  override def getPartitions: Array[Partition] = {
    objectIds.asScala.zipWithIndex.map { case (k, i) =>
      new RayDatasetRDDPartition(k, i).asInstanceOf[Partition]
    }.toArray
  }

  override def compute(split: Partition, context: TaskContext): Iterator[Array[Byte]] = {
    val ref = split.asInstanceOf[RayDatasetRDDPartition].ref
    ObjectStoreReader.getBatchesFromStream(ref, locations.get(split.index))
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    Seq(Address.parseFrom(locations.get(split.index)).getIpAddress())
  }
}
