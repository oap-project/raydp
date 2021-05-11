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
