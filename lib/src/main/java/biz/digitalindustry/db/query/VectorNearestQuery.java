package biz.digitalindustry.db.query;

import biz.digitalindustry.db.model.Vector;

public record VectorNearestQuery(
        String table,
        String field,
        Vector vector,
        int limit
) {
    public VectorNearestQuery {
        if (table == null || table.isBlank()) {
            throw new IllegalArgumentException("table must not be blank");
        }
        if (field == null || field.isBlank()) {
            throw new IllegalArgumentException("field must not be blank");
        }
        if (vector == null) {
            throw new IllegalArgumentException("vector must not be null");
        }
        if (limit <= 0) {
            throw new IllegalArgumentException("limit must be positive");
        }
    }
}
