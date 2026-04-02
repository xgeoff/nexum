package biz.digitalindustry.storage.model;

import biz.digitalindustry.storage.schema.EntityType;

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
