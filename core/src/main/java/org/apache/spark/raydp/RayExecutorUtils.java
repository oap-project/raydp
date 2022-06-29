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

package org.apache.spark.raydp;

import io.ray.api.ActorHandle;
import io.ray.api.Ray;
import io.ray.api.call.ActorCreator;
import java.util.Map;
import java.util.List;

import io.ray.api.placementgroup.PlacementGroup;
import io.ray.runtime.object.ObjectRefImpl;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.Partition;
import org.apache.spark.TaskContext;
import org.apache.spark.executor.RayCoarseGrainedExecutorBackend;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.Row;


public class RayExecutorUtils {
  /**
   * Convert from mbs -> memory units. The memory units in ray is byte
   */

  private static double toMemoryUnits(int memoryInMB) {
    double result = 1.0 * memoryInMB * 1024 * 1024;
    return Math.round(result);
  }

  public static ActorHandle<RayCoarseGrainedExecutorBackend> createExecutorActor(
      String executorId,
      String appMasterURL,
      int cores,
      int memoryInMB,
      Map<String, Double> resources,
      PlacementGroup placementGroup,
      int bundleIndex,
      List<String> javaOpts) {
    ActorCreator<RayCoarseGrainedExecutorBackend> creator = Ray.actor(
            RayCoarseGrainedExecutorBackend::new, executorId, appMasterURL);
    creator.setName("raydp-executor-" + executorId);
    creator.setJvmOptions(javaOpts);
    creator.setResource("CPU", (double)cores);
    creator.setResource("memory", toMemoryUnits(memoryInMB));
    for (Map.Entry<String, Double> entry: resources.entrySet()) {
      creator.setResource(entry.getKey(), entry.getValue());
    }
    if (placementGroup != null) {
      creator.setPlacementGroup(placementGroup, bundleIndex);
    }

    return creator.remote();
  }

  public static void setUpExecutor(
      ActorHandle<RayCoarseGrainedExecutorBackend> handler,
      String appId,
      String driverUrl,
      int cores,
      String classPathEntries) {
    handler.task(RayCoarseGrainedExecutorBackend::startUp,
        appId, driverUrl, cores, classPathEntries).remote();
  }

  public static String[] getBlockLocations(
      ActorHandle<RayCoarseGrainedExecutorBackend> handler,
      int rddId,
      int numPartitions) {
    return handler.task(RayCoarseGrainedExecutorBackend::getBlockLocations,
        rddId, numPartitions).remote().get();
  }

  public static byte[] getRDDPartition(
      ActorHandle<RayCoarseGrainedExecutorBackend> handler,
      RDD<byte[]> rdd,
      Partition partition,
      Schema schema) {
    ObjectRefImpl ref = (ObjectRefImpl<byte[]>) handler.task(
        RayCoarseGrainedExecutorBackend::getRDDPartition,
        rdd, partition, schema.toJson()).remote();
    return ref.getId().getBytes();
  }
}
