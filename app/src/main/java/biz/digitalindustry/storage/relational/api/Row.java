package biz.digitalindustry.storage.relational.api;

import biz.digitalindustry.storage.model.FieldValue;

import java.util.Map;

public record Row(
        String primaryKey,
        Map<String, FieldValue> values
) {
}
