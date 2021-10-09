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

import io.ray.api.ActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;

public class RayAppMasterUtils {
  public static ActorHandle<RayAppMaster> createAppMaster(
      String cp, List<String> jvmOptions) {
    return Ray.actor(RayAppMaster::new, cp).setJvmOptions(jvmOptions).remote();
  }

  public static String getMasterUrl(
      ActorHandle<RayAppMaster> handle) {
    return handle.task(RayAppMaster::getMasterUrl).remote().get();
  }

  public static void stopAppMaster(
      ActorHandle<RayAppMaster> handle) {
    handle.task(RayAppMaster::stop).remote().get();
    handle.kill();
  }

  public static String RAYDP_OWNER_NAME = "RAYDP_OBJECT_OWNER";

  public static ActorHandle<RayDPObjectOwner> createObjectOwner() {
    return Ray.actor(RayDPObjectOwner::new)
        .setGlobalName(RAYDP_OWNER_NAME)
        .remote();
  }

  public static void addObjectRef(ActorHandle<RayDPObjectOwner> owner,
      ObjectRef<ObjectRef<byte[]>> ref) {
    owner.task(RayDPObjectOwner::addObjectRef, ref).remote();
  }
}
