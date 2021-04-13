package org.apache.spark.deploy.raydp;

import org.apache.spark.deploy.raydp.RayExternalShuffleService;
import org.apache.spark.SparkConf;
import org.apache.spark.SecurityManager;
import io.ray.api.ActorHandle;
import io.ray.api.Ray;

public class ExternalShuffleServiceUtils {
  public static ActorHandle<RayExternalShuffleService> createShuffleService(
      String node) {
    return Ray.actor(RayExternalShuffleService::new)
              .setResource(node, 0.01).remote();
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
