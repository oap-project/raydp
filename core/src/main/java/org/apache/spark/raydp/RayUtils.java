package org.apache.spark.raydp;

import io.ray.api.ActorHandle;
import io.ray.api.Ray;
import io.ray.api.call.ActorCreator;
import org.apache.spark.deploy.raydp.RayAppMaster;
import org.apache.spark.executor.RayCoarseGrainedExecutorBackend;

import java.util.Map;

public class RayUtils {
  private static int MEMORY_RESOURCE_UNIT_BYTES = 50 * 1024 * 1024;
  /**
   * Convert from mbs -> memory units. The memory units in ray is 50 * 1024 * 1024
   */
  private static double toMemoryUnits(int memoryInMB) {
    double result = 1.0 * memoryInMB * 1024 * 1024 / MEMORY_RESOURCE_UNIT_BYTES;
    return Math.round(result);
  }

  public static ActorHandle<RayAppMaster> createAppMaster(
      int cores,
      int memoryInMB) {
    ActorCreator<RayAppMaster> creator = Ray.actor(RayAppMaster::new);
    creator.setResource("CPU", (double)cores);
    creator.setResource("memory", toMemoryUnits(memoryInMB));
    return creator.remote();
  }

  public static String getMasterUrl(
      ActorHandle<RayAppMaster> master) {
    return master.task(RayAppMaster::getMasterUrl).remote().get();
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
