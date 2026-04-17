package biz.digitalindustry.db.server.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.micronaut.core.annotation.Introspected;
import java.util.Map;

@Introspected
public class Node {
    private String id;
    private Map<String, Object> properties;

    public Node() {
    }

    @JsonCreator
    public Node(@JsonProperty("id") String id,
                @JsonProperty("properties") Map<String, Object> properties) {
        this.id = id;
        this.properties = properties;
    }

    public String getId() {
        return id;
    }

    public Map<String, Object> getProperties() {
        return properties;
    }
}
