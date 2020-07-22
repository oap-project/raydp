package org.apache.spark.raydp;

import io.ray.api.Ray;
import org.apache.spark.deploy.raydp.RayAppMaster;

public class PyjniusBridge {
    private RayAppMaster instance = null;
    public void createAppMaster() {
        if (instance == null) {
            // init ray, we should set the config by java properties
            Ray.init();
            instance = new RayAppMaster();
        }
    }

    public String getMasterUrl() {
        if (instance == null) {
            throw new RuntimeException("You should create the RayAppMaster instance first");
        }

        return instance.getMasterUrl();
    }

    public void stop() {
        if (instance != null) {
            instance.stop();
            instance = null;
        }
    }
}
