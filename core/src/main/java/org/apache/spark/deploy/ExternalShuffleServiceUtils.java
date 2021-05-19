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

package org.apache.spark.deploy.raydp;

import io.ray.api.ActorHandle;
import io.ray.api.Ray;

public class ExternalShuffleServiceUtils {
  public static ActorHandle<RayExternalShuffleService> createShuffleService(
      String node) {
    return Ray.actor(RayExternalShuffleService::new)
              .setResource("node:" + node, 0.01).remote();
  }

  public static void startShuffleService(
      ActorHandle<RayExternalShuffleService> handle) {
    handle.task(RayExternalShuffleService::start).remote();
  }

  public static void stopShuffleService(
      ActorHandle<RayExternalShuffleService> handle) {
    handle.task(RayExternalShuffleService::stop).remote();
  }
}
