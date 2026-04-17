package biz.digitalindustry.db.query;

import java.util.Map;

public record QueryNode(String id, Map<String, Object> properties) {
    public QueryNode {
        properties = Map.copyOf(properties);
    }
}
