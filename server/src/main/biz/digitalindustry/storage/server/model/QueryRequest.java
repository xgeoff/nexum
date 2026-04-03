package biz.digitalindustry.storage.server.model;

import com.fasterxml.jackson.annotation.JsonAnySetter;
import io.micronaut.core.annotation.Introspected;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Represents a query request where the query type is encoded as the key
 * of the single entry in the JSON body.
 */
@Introspected
public class QueryRequest {
    private final Map<String, Object> queries = new LinkedHashMap<>();

    public QueryRequest() {
    }

    /**
     * Adds a query of the given type. Used for JSON deserialization and tests.
     */
    @JsonAnySetter
    public void setQuery(String name, Object query) {
        if (name != null) {
            queries.put(name, query);
        }
    }

    /**
     * Convenience factory for tests.
     */
    public static QueryRequest of(String type, String query) {
        return of(type, (Object) query);
    }

    public static QueryRequest of(String type, Object payload) {
        QueryRequest req = new QueryRequest();
        req.setQuery(type, payload);
        return req;
    }

    /**
     * Returns the first query type supplied or {@code null} if none.
     */
    public String queryType() {
        return queries.keySet().stream().findFirst().orElse(null);
    }

    /**
     * Returns the query string for the first query type or {@code null} if none.
     */
    public Object query() {
        String type = queryType();
        return type == null ? null : queries.get(type);
    }

    public Map<String, Object> getQueries() {
        return queries;
    }
}
