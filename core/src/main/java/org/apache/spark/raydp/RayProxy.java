package org.apache.spark.raydp;

import io.ray.api.call.ActorCreator;
import io.ray.api.function.RayFunc2;

import io.ray.api.Ray;
import io.ray.api.ActorHandle;
import org.apache.spark.executor.RayCoarseGrainedExecutorBackend;

import java.util.Map;

public class RayProxy {
    public static ActorHandle<RayCoarseGrainedExecutorBackend> createExecutorActor(
        String executorId,
        String masterURL,
        int cores,
        int memoryInMB,
        Map<String, Double> resources,
        String javaOpts) {
      ActorCreator<RayCoarseGrainedExecutorBackend> handler = Ray.actor(new RayFunc2() {
        @Override
        public Object apply(Object executorId, Object url) throws Exception {
          String id = (String)executorId;
          String masterUrl = (String)url;
          return new RayCoarseGrainedExecutorBackend(id, masterUrl);
        }
      }, executorId, masterURL);

      handler.setJvmOptions(javaOpts);
      handler.setResource("CPU", (double)cores);
      handler.setResource("MEM", (double)(memoryInMB * 1024 * 1024));
      for (Map.Entry<String, Double> entry: resources.entrySet()) {
          handler.setResource(entry.getKey(), entry.getValue());
      }

      return handler.remote();
    }
}
