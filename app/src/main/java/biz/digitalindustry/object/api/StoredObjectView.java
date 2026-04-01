package biz.digitalindustry.object.api;

import biz.digitalindustry.storage.model.FieldValue;

import java.util.Map;

public record StoredObjectView(
        String key,
        Map<String, FieldValue> fields
) {
}
