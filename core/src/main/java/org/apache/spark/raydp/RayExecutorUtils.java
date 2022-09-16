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
import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import io.ray.api.call.ActorCreator;
import java.util.Map;
import java.util.List;

import io.ray.api.placementgroup.PlacementGroup;
import io.ray.runtime.object.ObjectRefImpl;
import org.apache.spark.executor.RayCoarseGrainedExecutorBackend;


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
      double cores,
      int memoryInMB,
      Map<String, Double> resources,
      PlacementGroup placementGroup,
      int bundleIndex,
      List<String> javaOpts) {
    ActorCreator<RayCoarseGrainedExecutorBackend> creator = Ray.actor(
            RayCoarseGrainedExecutorBackend::new, executorId, appMasterURL);
    creator.setName("raydp-executor-" + executorId);
    creator.setJvmOptions(javaOpts);
    creator.setResource("CPU", cores);
    creator.setResource("memory", toMemoryUnits(memoryInMB));

    for (Map.Entry<String, Double> entry: resources.entrySet()) {
      creator.setResource(entry.getKey(), entry.getValue());
    }
    if (placementGroup != null) {
      creator.setPlacementGroup(placementGroup, bundleIndex);
    }
    creator.setMaxRestarts(3);
    creator.setMaxTaskRetries(3);
    creator.setMaxConcurrency(2);
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

  public static ObjectRef<byte[]> getRDDPartition(
      ActorHandle<RayCoarseGrainedExecutorBackend> handle,
      int rddId,
      int partitionId,
      String schema,
      String driverAgentUrl) {
    return (ObjectRefImpl<byte[]>) handle.task(
        RayCoarseGrainedExecutorBackend::getRDDPartition,
        rddId, partitionId, schema, driverAgentUrl).remote();
  }

  public static void exitExecutor(
    ActorHandle<RayCoarseGrainedExecutorBackend> handle
  ) {
    handle.task(RayCoarseGrainedExecutorBackend::stop).remote();
  }
}
