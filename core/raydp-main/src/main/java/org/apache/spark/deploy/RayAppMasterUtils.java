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

import java.util.List;
import java.util.Map;

import io.ray.api.ActorHandle;
import io.ray.api.Ray;
import io.ray.api.call.ActorCreator;

public class RayAppMasterUtils {
  public static ActorHandle<RayAppMaster> createAppMaster(
      String cp,
      String name,
      List<String> jvmOptions) {
    ActorCreator<RayAppMaster> creator = Ray.actor(RayAppMaster::new, cp);
    if (name != null) {
      creator.setName(name);
    }
    jvmOptions.add("-cp");
    jvmOptions.add(cp);
    creator.setJvmOptions(jvmOptions);
    return creator.remote();
  }

  public static String getMasterUrl(
      ActorHandle<RayAppMaster> handle) {
    return handle.task(RayAppMaster::getMasterUrl).remote().get();
  }

  public static Map<String, String> getRestartedExecutors(
      ActorHandle<RayAppMaster> handle) {
    return handle.task(RayAppMaster::getRestartedExecutors).remote().get();
  }

  public static void stopAppMaster(
      ActorHandle<RayAppMaster> handle) {
    handle.task(RayAppMaster::stop).remote().get();
    handle.kill();
  }
}
