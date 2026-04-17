package biz.digitalindustry.db.vector.api;

import biz.digitalindustry.db.model.FieldValue;

import java.util.Map;

public record VectorDocument(
        String key,
        Map<String, FieldValue> values
) {
    public VectorDocument {
        values = Map.copyOf(values);
    }
}
