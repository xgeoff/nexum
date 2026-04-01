package biz.digitalindustry.storage.object.api;

import biz.digitalindustry.storage.model.RecordId;

public record StoredObject<T>(
        RecordId id,
        String key,
        T value
) {
}
