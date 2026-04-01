package biz.digitalindustry.storage.object.api;

import biz.digitalindustry.storage.model.ReferenceValue;

public interface ObjectStoreContext {
    ReferenceValue reference(ObjectType<?> type, String key);

    <T> T resolve(ObjectType<T> type, ReferenceValue reference);
}
