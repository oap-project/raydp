package org.apache.spark.raydp;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.ray.api.Ray;
import org.apache.spark.deploy.raydp.RayAppMaster;

import java.util.*;

public class AppMasterJavaBridge {
    private RayAppMaster instance = null;

    public AppMasterJavaBridge(String properties) {
        ObjectMapper mapper = new ObjectMapper();
        TypeReference<HashMap<String, String>> typeRef
                = new TypeReference<HashMap<String, String>>() {};
        HashMap<String, String> propertiesMap = null;
        try {
            propertiesMap = mapper.readValue(properties, typeRef);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        for (Map.Entry<String, String> pair: propertiesMap.entrySet()) {
            System.setProperty(pair.getKey(), pair.getValue());
        }
    }

    public void createAppMaster(String extra_cp) {
        if (instance == null) {
            // init ray, we should set the config by java properties
            Ray.init();
            System.out.println(Ray.getRuntimeContext().getCurrentJobId());
            instance = new RayAppMaster(extra_cp);
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
