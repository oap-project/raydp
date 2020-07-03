package org.apache.spark.raydp;

import io.ray.api.call.ActorCreator;
import io.ray.api.function.RayFunc;
import io.ray.api.function.RayFunc0;
import io.ray.api.function.RayFunc2;

import io.ray.api.Ray;
import io.ray.api.ActorHandle;
import org.apache.spark.deploy.raydp.RayAppMaster;
import org.apache.spark.deploy.raydp.RayAppMaster$;
import org.apache.spark.executor.RayCoarseGrainedExecutorBackend;

import java.util.Map;

public class RayProxy {
    public static ActorHandle<RayAppMaster> createAppMaster(
        int cores,
        int memoryInMB) {
      ActorCreator<RayAppMaster> creator = Ray.actor(new RayFunc0() {
        @Override
        public RayAppMaster apply() throws Exception {
          return RayAppMaster$.MODULE$.apply();
        }
      });
      creator.setResource("CPU", (double)cores);
      creator.setResource("MEM", (double)(memoryInMB * 1024 * 1024));
      return creator.remote();
    }

    public static ActorHandle<RayCoarseGrainedExecutorBackend> createExecutorActor(
        String executorId,
        String masterURL,
        int cores,
        int memoryInMB,
        Map<String, Double> resources,
        String javaOpts) {
      ActorCreator<RayCoarseGrainedExecutorBackend> creator = Ray.actor(new RayFunc2() {
        @Override
        public Object apply(Object executorId, Object url) throws Exception {
          String id = (String)executorId;
          String masterUrl = (String)url;
          return new RayCoarseGrainedExecutorBackend(id, masterUrl);
        }
      }, executorId, masterURL);

      creator.setJvmOptions(javaOpts);
      creator.setResource("CPU", (double)cores);
      creator.setResource("MEM", (double)(memoryInMB * 1024 * 1024));
      for (Map.Entry<String, Double> entry: resources.entrySet()) {
        creator.setResource(entry.getKey(), entry.getValue());
      }

      return creator.remote();
    }
}
