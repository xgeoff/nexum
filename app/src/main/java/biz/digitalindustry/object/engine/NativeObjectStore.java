package biz.digitalindustry.object.engine;

import biz.digitalindustry.object.api.ObjectStore;
import biz.digitalindustry.object.api.ObjectStoreContext;
import biz.digitalindustry.object.api.ObjectType;
import biz.digitalindustry.object.api.StoredObject;
import biz.digitalindustry.object.api.StoredObjectView;
import biz.digitalindustry.storage.api.StorageConfig;
import biz.digitalindustry.storage.engine.ExactMatchIndexManager;
import biz.digitalindustry.storage.engine.NativeStorageEngine;
import biz.digitalindustry.storage.model.FieldValue;
import biz.digitalindustry.storage.model.Record;
import biz.digitalindustry.storage.model.RecordId;
import biz.digitalindustry.storage.model.ReferenceValue;
import biz.digitalindustry.storage.model.StringValue;
import biz.digitalindustry.storage.store.RecordStore;
import biz.digitalindustry.storage.tx.Transaction;
import biz.digitalindustry.storage.tx.TransactionMode;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class NativeObjectStore implements ObjectStore {
    private static final int DEFAULT_PAGE_SIZE = 8192;

    private final NativeStorageEngine storageEngine;
    private final Map<String, ObjectType<?>> types = new LinkedHashMap<>();

    public NativeObjectStore(String path) {
        this.storageEngine = new NativeStorageEngine();
        this.storageEngine.open(new StorageConfig(path, DEFAULT_PAGE_SIZE));
    }

    @Override
    public synchronized <T> void registerType(ObjectType<T> type) {
        types.put(type.name(), type);
        storageEngine.schemaRegistry().register(type.entityType());
        rebuildIndexes(type);
    }

    @Override
    public synchronized <T> StoredObject<T> save(ObjectType<T> type, T object) {
        requireRegistered(type);
        try (Transaction tx = storageEngine.begin(TransactionMode.READ_WRITE)) {
            String key = type.codec().key(object);
            Record existing = findRecordByKey(type, key);
            RecordStore recordStore = storageEngine.recordStore();
            Map<String, FieldValue> encodedFields = withObjectKey(key, type.codec().encode(object, new NativeObjectStoreContext()));
            Record stored;
            if (existing == null) {
                stored = recordStore.create(type.entityType(), new Record(null, type.entityType(), encodedFields));
            } else {
                stored = new Record(existing.id(), type.entityType(), encodedFields);
                recordStore.update(stored);
                removeFromIndexes(type, existing);
            }
            addToIndexes(type, stored);
            tx.commit();
            return toStoredObject(type, stored);
        }
    }

    @Override
    public synchronized <T> StoredObject<T> get(ObjectType<T> type, String key) {
        requireRegistered(type);
        return toStoredObject(type, findRecordByKey(type, key));
    }

    @Override
    public synchronized <T> List<StoredObject<T>> getAll(ObjectType<T> type) {
        requireRegistered(type);
        List<StoredObject<T>> objects = new ArrayList<>();
        for (Record record : storageEngine.recordStore().scan(type.entityType())) {
            objects.add(toStoredObject(type, record));
        }
        return objects;
    }

    @Override
    public synchronized <T> StoredObject<T> findOneBy(ObjectType<T> type, String fieldName, FieldValue value) {
        requireRegistered(type);
        validateField(type, fieldName);
        for (Record record : findRecords(type, fieldName, value)) {
            if (record != null) {
                return toStoredObject(type, record);
            }
        }
        return null;
    }

    @Override
    public synchronized <T> List<StoredObject<T>> findBy(ObjectType<T> type, String fieldName, FieldValue value) {
        requireRegistered(type);
        validateField(type, fieldName);
        List<StoredObject<T>> matches = new ArrayList<>();
        for (Record record : findRecords(type, fieldName, value)) {
            if (record != null) {
                matches.add(toStoredObject(type, record));
            }
        }
        return matches;
    }

    @Override
    public synchronized <T> boolean delete(ObjectType<T> type, String key) {
        requireRegistered(type);
        try (Transaction tx = storageEngine.begin(TransactionMode.READ_WRITE)) {
            Record existing = findRecordByKey(type, key);
            if (existing == null) {
                tx.rollback();
                return false;
            }
            boolean deleted = storageEngine.recordStore().delete(existing.id());
            removeFromIndexes(type, existing);
            tx.commit();
            return deleted;
        }
    }

    @Override
    public synchronized void close() {
        storageEngine.close();
    }

    private Map<String, FieldValue> withObjectKey(String key, Map<String, FieldValue> fields) {
        Map<String, FieldValue> stored = new LinkedHashMap<>();
        stored.put(ObjectType.KEY_FIELD, new StringValue(key));
        stored.putAll(fields);
        return stored;
    }

    private <T> void requireRegistered(ObjectType<T> type) {
        if (types.get(type.name()) != type) {
            throw new IllegalStateException("Object type '" + type.name() + "' is not registered in this store");
        }
    }

    private <T> Record findRecordByKey(ObjectType<T> type, String key) {
        for (Record record : findRecords(type, ObjectType.KEY_FIELD, new StringValue(key))) {
            if (record != null) {
                return record;
            }
        }
        return null;
    }

    private <T> List<Record> findRecords(ObjectType<T> type, String fieldName, FieldValue value) {
        if (indexManager().hasField(indexNamespace(type), fieldName)) {
            List<Record> records = new ArrayList<>();
            for (Object id : storageEngine.exactIndexFind(indexNamespace(type), fieldName, value)) {
                records.add(findRecordById((RecordId) id));
            }
            return records;
        }

        List<Record> scanned = new ArrayList<>();
        for (Record record : storageEngine.recordStore().scan(type.entityType())) {
            if (matchesField(record, fieldName, value)) {
                scanned.add(record);
            }
        }
        return scanned;
    }

    private <T> void validateField(ObjectType<T> type, String fieldName) {
        if (ObjectType.KEY_FIELD.equals(fieldName)) {
            return;
        }
        for (var field : type.fields()) {
            if (field.name().equals(fieldName)) {
                return;
            }
        }
        throw new IllegalArgumentException("Unknown field '" + fieldName + "' for object type '" + type.name() + "'");
    }

    private boolean matchesField(Record record, String fieldName, FieldValue expected) {
        FieldValue actual = record.fields().get(fieldName);
        return actual == null ? expected == null : actual.equals(expected);
    }

    private <T> void rebuildIndexes(ObjectType<T> type) {
        storageEngine.ensureExactMatchNamespace(
                indexNamespace(type),
                indexedFields(type),
                type.entityType(),
                Record::id
        );
    }

    private <T> List<String> indexedFields(ObjectType<T> type) {
        List<String> fields = new ArrayList<>();
        fields.add(ObjectType.KEY_FIELD);
        for (var index : type.indexes()) {
            if (index.fields().size() == 1) {
                String fieldName = index.fields().get(0);
                if (!fields.contains(fieldName)) {
                    fields.add(fieldName);
                }
            }
        }
        return fields;
    }

    private <T> void addToIndexes(ObjectType<T> type, Record record) {
        storageEngine.exactIndexAdd(indexNamespace(type), indexedFields(type), record.fields(), record.id());
    }

    private <T> void removeFromIndexes(ObjectType<T> type, Record record) {
        storageEngine.exactIndexRemove(indexNamespace(type), indexedFields(type), record.fields(), record.id());
    }

    private ExactMatchIndexManager indexManager() {
        return storageEngine.indexManager();
    }

    private <T> String indexNamespace(ObjectType<T> type) {
        return "object:" + type.name();
    }

    private Record findRecordById(RecordId id) {
        return storageEngine.recordStore().get(id);
    }

    private String stringField(Record record, String field) {
        FieldValue value = record.fields().get(field);
        return value instanceof StringValue stringValue ? stringValue.value() : null;
    }

    private <T> StoredObject<T> toStoredObject(ObjectType<T> type, Record record) {
        if (record == null) {
            return null;
        }
        String key = stringField(record, ObjectType.KEY_FIELD);
        T value = type.codec().decode(new StoredObjectView(key, record.fields()), new NativeObjectStoreContext());
        return new StoredObject<>(record.id(), key, value);
    }

    private final class NativeObjectStoreContext implements ObjectStoreContext {
        @Override
        public ReferenceValue reference(ObjectType<?> type, String key) {
            requireRegistered(type);
            Record target = findRecordByKey(type, key);
            if (target == null) {
                throw new IllegalArgumentException("Referenced object '" + type.name() + ":" + key + "' does not exist");
            }
            return new ReferenceValue(target.id());
        }

        @Override
        public <T> T resolve(ObjectType<T> type, ReferenceValue reference) {
            if (reference == null) {
                return null;
            }
            requireRegistered(type);
            Record target = findRecordById(reference.recordId());
            if (target == null) {
                return null;
            }
            if (!type.entityType().name().equals(target.type().name())) {
                throw new IllegalArgumentException("Reference points to '" + target.type().name() + "', expected '" + type.name() + "'");
            }
            StoredObject<T> stored = toStoredObject(type, target);
            return stored == null ? null : stored.value();
        }
    }
}
