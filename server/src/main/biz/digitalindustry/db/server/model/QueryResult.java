package biz.digitalindustry.db.server.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;

public class QueryResult {
    private Map<String, Node> result;

    public QueryResult() {
    }

    @JsonCreator
    public QueryResult(@JsonProperty("result") Map<String, Node> result) {
        this.result = result;
    }

    public Map<String, Node> getResult() {
        return result;
    }
}
