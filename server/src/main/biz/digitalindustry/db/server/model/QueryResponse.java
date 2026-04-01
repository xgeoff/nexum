package biz.digitalindustry.db.server.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.micronaut.core.annotation.Introspected;
import java.util.*;

@Introspected
public class QueryResponse {
    private List<Map<String, Node>> results;

    public QueryResponse() {
        this.results = new ArrayList<>();
    }

    @JsonCreator
    public QueryResponse(@JsonProperty("results") List<Map<String, Node>> results) {
        this.results = results == null ? new ArrayList<>() : results;
    }

    public List<Map<String, Node>> getResults() {
        if (results == null) {
            results = new ArrayList<>();
        }
        return results;
    }
}
