package biz.digitalindustry.db.model;

import biz.digitalindustry.db.schema.EntityType;

import java.util.Map;

public record Record(
        RecordId id,
        EntityType type,
        Map<String, FieldValue> fields
) {
    public Record {
        fields = Map.copyOf(fields);
    }
}
