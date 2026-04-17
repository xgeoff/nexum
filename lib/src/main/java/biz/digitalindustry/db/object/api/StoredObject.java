package biz.digitalindustry.db.object.api;

import biz.digitalindustry.db.model.RecordId;

public record StoredObject<T>(
        RecordId id,
        String key,
        T value
) {
}
