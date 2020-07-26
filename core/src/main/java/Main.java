import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.ray.api.Ray;

import java.util.HashMap;
import java.util.Map;

public class Main {
    public static void main(String[] args) {
        String properties = "{\"ray.node-ip\": \"192.168.3.5\", \"ray.redis.address\": \"192.168.3.5:6379\", \"ray.redis.password\": \"123\", \"ray.logging.dir\": \"/tmp/ray/session_2020-07-24_23-42-34_536515_30845\", \"ray.raylet.node-manager-port\": 63075, \"ray.raylet.socket-name\": \"/tmp/ray/session_2020-07-24_23-42-34_536515_30845/sockets/raylet\", \"ray.object-store.socket-name\": \"/tmp/ray/session_2020-07-24_23-42-34_536515_30845/sockets/plasma_store\"}";
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
        Ray.init();
        System.out.println(Ray.getRuntimeContext().getCurrentJobId());
        System.out.println(Ray.task(Main::get).remote().get());
    }

    public static int get() {
        return 1;
    }
}
