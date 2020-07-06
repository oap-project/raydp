package org.apache.spark.raydp;

import io.ray.api.ActorHandle;
import io.ray.api.Ray;
import io.ray.api.call.ActorCreator;
import org.apache.spark.deploy.raydp.RayAppMaster;
import org.apache.spark.executor.RayCoarseGrainedExecutorBackend;

import java.util.Map;

public class RayUtils {
  public static ActorHandle<RayAppMaster> createAppMaster(
      int cores,
      int memoryInMB) {
    ActorCreator<RayAppMaster> creator = Ray.actor(RayAppMaster::new);
    creator.setResource("CPU", (double)cores);
    creator.setResource("memory", (double)(memoryInMB * 1024 * 1024));
    return creator.remote();
  }

  public static String getMasterUrl(
      ActorHandle<RayAppMaster> master) {
    return master.task(RayAppMaster::getMasterUrl).remote().get();
  }

  public static ActorHandle<RayCoarseGrainedExecutorBackend> createExecutorActor(
      String executorId,
      String masterURL,
      int cores,
      int memoryInMB,
      Map<String, Double> resources,
      String javaOpts) {
    ActorCreator<RayCoarseGrainedExecutorBackend> creator = Ray.actor(
            RayCoarseGrainedExecutorBackend::new, executorId, masterURL);

    creator.setJvmOptions(javaOpts);
    creator.setResource("CPU", (double)cores);
    creator.setResource("memory", (double)(memoryInMB * 1024 * 1024));
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
