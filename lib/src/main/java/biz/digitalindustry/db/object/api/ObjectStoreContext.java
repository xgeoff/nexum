package biz.digitalindustry.db.object.api;

import biz.digitalindustry.db.model.ReferenceValue;

public interface ObjectStoreContext {
    ReferenceValue reference(ObjectType<?> type, String key);

    <T> T resolve(ObjectType<T> type, ReferenceValue reference);
}
