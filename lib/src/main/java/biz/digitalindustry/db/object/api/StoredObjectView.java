package biz.digitalindustry.db.object.api;

import biz.digitalindustry.db.model.FieldValue;

import java.util.Map;

public record StoredObjectView(
        String key,
        Map<String, FieldValue> fields
) {
}
