package org.apache.spark.deploy.raydp;

import io.ray.api.ActorHandle;
import io.ray.api.Ray;

public class RayAppMasterUtils {
  public static ActorHandle<RayAppMaster> createAppMaster(String cp) {
    return Ray.actor(RayAppMaster::new, cp).remote();
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
}
