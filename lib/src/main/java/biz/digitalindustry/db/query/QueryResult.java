package biz.digitalindustry.db.query;

import java.util.List;
import java.util.Map;

public record QueryResult(List<Map<String, QueryNode>> rows) {
    public QueryResult {
        rows = rows == null ? List.of() : List.copyOf(rows);
    }
}
