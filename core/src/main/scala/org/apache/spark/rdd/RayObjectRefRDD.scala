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
import org.apache.spark.raydp.RayDPUtils
import org.apache.spark.sql.Row

private[spark] class RayObjectRefRDDPartition(idx: Int) extends Partition {
  val index = idx
}

private[spark]
class RayObjectRefRDD(sc: SparkContext,
                    val objectIds: List[Array[Byte]],
                    val locations: List[Array[Byte]])
  extends RDD[Row](sc, Nil) {

  val refs = objectIds.asScala

  override def getPartitions: Array[Partition] = {
    (0 until refs.length).map { i =>
      new RayObjectRefRDDPartition(i).asInstanceOf[Partition]
    }.toArray
  }

  override def compute(split: Partition, context: TaskContext): Iterator[Row] = {
    (Row(refs(split.index)) :: Nil).iterator
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    Seq(Address.parseFrom(locations.get(split.index)).getIpAddress())
  }
}

