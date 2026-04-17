package biz.digitalindustry.db.relational.api;

import biz.digitalindustry.db.model.FieldValue;

import java.util.Map;

public record Row(
        String primaryKey,
        Map<String, FieldValue> values
) {
}
