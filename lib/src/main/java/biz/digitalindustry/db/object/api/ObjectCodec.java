package biz.digitalindustry.db.object.api;

import biz.digitalindustry.db.model.FieldValue;

import java.util.Map;

public interface ObjectCodec<T> {
    String key(T object);

    Map<String, FieldValue> encode(T object, ObjectStoreContext context);

    T decode(StoredObjectView view, ObjectStoreContext context);
}
