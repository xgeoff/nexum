package biz.digitalindustry.db.query;

import java.util.LinkedHashMap;
import java.util.Map;

public record QueryCommand(String queryType, Map<String, Object> payload) {
    public QueryCommand {
        payload = payload == null ? Map.of() : Map.copyOf(new LinkedHashMap<>(payload));
    }
}
