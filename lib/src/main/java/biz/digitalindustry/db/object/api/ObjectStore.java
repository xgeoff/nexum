package biz.digitalindustry.db.object.api;

import biz.digitalindustry.db.model.FieldValue;

import java.util.List;

public interface ObjectStore extends AutoCloseable {
    <T> void registerType(ObjectType<T> type);

    <T> StoredObject<T> save(ObjectType<T> type, T object);

    <T> StoredObject<T> get(ObjectType<T> type, String key);

    <T> List<StoredObject<T>> getAll(ObjectType<T> type);

    <T> StoredObject<T> findOneBy(ObjectType<T> type, String fieldName, FieldValue value);

    <T> List<StoredObject<T>> findBy(ObjectType<T> type, String fieldName, FieldValue value);

    <T> boolean delete(ObjectType<T> type, String key);

    @Override
    void close();
}
