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
import org.apache.spark.executor.RayCoarseGrainedExecutorBackend;

import java.util.Map;

public class AppMasterJavaUtils {
  private static int MEMORY_RESOURCE_UNIT_BYTES = 50 * 1024 * 1024;
  /**
   * Convert from mbs -> memory units. The memory units in ray is 50 * 1024 * 1024
   */
  private static double toMemoryUnits(int memoryInMB) {
    double result = 1.0 * memoryInMB * 1024 * 1024 / MEMORY_RESOURCE_UNIT_BYTES;
    return Math.round(result);
  }

  public static ActorHandle<RayCoarseGrainedExecutorBackend> createExecutorActor(
      String executorId,
      String appMasterURL,
      int cores,
      int memoryInMB,
      Map<String, Double> resources,
      String javaOpts) {
    ActorCreator<RayCoarseGrainedExecutorBackend> creator = Ray.actor(
            RayCoarseGrainedExecutorBackend::new, executorId, appMasterURL);

    creator.setJvmOptions(javaOpts);
    creator.setResource("CPU", (double)cores);
    creator.setResource("memory", toMemoryUnits(memoryInMB));
    for (Map.Entry<String, Double> entry: resources.entrySet()) {
      creator.setResource(entry.getKey(), entry.getValue());
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
}
