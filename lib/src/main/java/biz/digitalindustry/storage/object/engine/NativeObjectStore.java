package biz.digitalindustry.storage.object.engine;

import biz.digitalindustry.storage.object.api.ObjectStore;
import biz.digitalindustry.storage.object.api.ObjectStoreContext;
import biz.digitalindustry.storage.object.api.ObjectFieldDefinition;
import biz.digitalindustry.storage.object.api.ObjectTypeDefinition;
import biz.digitalindustry.storage.object.api.ObjectType;
import biz.digitalindustry.storage.object.api.GeneratedObjectTypes;
import biz.digitalindustry.storage.object.api.StoredObject;
import biz.digitalindustry.storage.object.api.StoredObjectView;
import biz.digitalindustry.storage.api.StorageConfig;
import biz.digitalindustry.storage.engine.NativeStorageEngine;
import biz.digitalindustry.storage.model.FieldValue;
import biz.digitalindustry.storage.model.BooleanValue;
import biz.digitalindustry.storage.model.ListValue;
import biz.digitalindustry.storage.model.NullValue;
import biz.digitalindustry.storage.model.Record;
import biz.digitalindustry.storage.model.RecordId;
import biz.digitalindustry.storage.model.ReferenceValue;
import biz.digitalindustry.storage.model.StringValue;
import biz.digitalindustry.storage.schema.EntityType;
import biz.digitalindustry.storage.schema.FieldDefinition;
import biz.digitalindustry.storage.schema.IndexDefinition;
import biz.digitalindustry.storage.schema.IndexKind;
import biz.digitalindustry.storage.schema.ValueType;
import biz.digitalindustry.storage.store.RecordStore;
import biz.digitalindustry.storage.tx.Transaction;
import biz.digitalindustry.storage.tx.TransactionMode;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

public class NativeObjectStore implements ObjectStore {
    private static final int DEFAULT_PAGE_SIZE = 8192;
    private static final EntityType OBJECT_TYPE_METADATA = new EntityType(
            "__nexum_object_type",
            List.of(
                    new FieldDefinition("typeName", ValueType.STRING, true, false),
                    new FieldDefinition("keyField", ValueType.STRING, true, false)
            ),
            List.of()
    );
    private static final EntityType OBJECT_FIELD_METADATA = new EntityType(
            "__nexum_object_field",
            List.of(
                    new FieldDefinition("typeName", ValueType.STRING, true, false),
                    new FieldDefinition("fieldName", ValueType.STRING, true, false),
                    new FieldDefinition("valueType", ValueType.STRING, true, false),
                    new FieldDefinition("required", ValueType.BOOLEAN, true, false),
                    new FieldDefinition("repeated", ValueType.BOOLEAN, true, false),
                    new FieldDefinition("referenceTarget", ValueType.STRING, false, false)
            ),
            List.of()
    );
    private static final EntityType OBJECT_INDEX_METADATA = new EntityType(
            "__nexum_object_index",
            List.of(
                    new FieldDefinition("typeName", ValueType.STRING, true, false),
                    new FieldDefinition("indexName", ValueType.STRING, true, false),
                    new FieldDefinition("indexKind", ValueType.STRING, true, false),
                    new FieldDefinition("fieldNames", ValueType.LIST, true, false)
            ),
            List.of()
    );

    private final NativeStorageEngine storageEngine;
    private final Map<String, ObjectType<?>> types = new ConcurrentHashMap<>();
    private final Map<String, ObjectTypeDefinition> generatedDefinitions = new ConcurrentHashMap<>();
    private final ReentrantLock writeLock = new ReentrantLock();

    public NativeObjectStore(String path) {
        this(StorageConfig.file(path, DEFAULT_PAGE_SIZE));
    }

    public NativeObjectStore(StorageConfig config) {
        this.storageEngine = new NativeStorageEngine();
        this.storageEngine.open(config);
        initializeMetadataSchema();
        loadPersistedGeneratedTypes();
    }

    public static NativeObjectStore fileBacked(String path) {
        return new NativeObjectStore(StorageConfig.file(path, DEFAULT_PAGE_SIZE));
    }

    public static NativeObjectStore memoryOnly() {
        return new NativeObjectStore(StorageConfig.memory("memory:nexum-object", DEFAULT_PAGE_SIZE, StorageConfig.DEFAULT_MAX_WAL_BYTES));
    }

    @Override
    public <T> void registerType(ObjectType<T> type) {
        writeLocked(() -> {
            types.put(type.name(), type);
            storageEngine.schemaRegistry().register(type.entityType());
            rebuildIndexes(type);
            return null;
        });
    }

    public ObjectType<Map<String, Object>> registerGeneratedType(ObjectTypeDefinition definition) {
        return writeLocked(() -> {
            GeneratedObjectTypes.validateDefinition(definition);
            ObjectTypeDefinition existing = generatedDefinitions.get(definition.name());
            if (existing != null && !existing.equals(definition)) {
                throw new IllegalArgumentException("Generated object type '" + definition.name() + "' is already registered with a different definition");
            }
            if (existing == null) {
                persistDefinition(definition);
            }
            generatedDefinitions.put(definition.name(), definition);
            ObjectType<Map<String, Object>> type = GeneratedObjectTypes.create(
                    definition,
                    this::generatedType,
                    this::generatedTypeDefinition
            );
            registerType(type);
            return type;
        });
    }

    public List<ObjectTypeDefinition> generatedTypeDefinitions() {
        return readLocked(() -> List.copyOf(generatedDefinitions.values()));
    }

    public ObjectTypeDefinition generatedTypeDefinition(String name) {
        return readLocked(() -> generatedDefinitions.get(name));
    }

    @SuppressWarnings("unchecked")
    public ObjectType<Map<String, Object>> generatedType(String name) {
        return readLocked(() -> (ObjectType<Map<String, Object>>) types.get(name));
    }

    @Override
    public <T> StoredObject<T> save(ObjectType<T> type, T object) {
        return writeLocked(() -> {
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
        });
    }

    @Override
    public <T> StoredObject<T> get(ObjectType<T> type, String key) {
        return readLocked(() -> {
            requireRegistered(type);
            return toStoredObject(type, findRecordByKey(type, key));
        });
    }

    @Override
    public <T> List<StoredObject<T>> getAll(ObjectType<T> type) {
        return readLocked(() -> {
            requireRegistered(type);
            List<StoredObject<T>> objects = new ArrayList<>();
            for (Record record : storageEngine.recordStore().scan(type.entityType())) {
                objects.add(toStoredObject(type, record));
            }
            return objects;
        });
    }

    @Override
    public <T> StoredObject<T> findOneBy(ObjectType<T> type, String fieldName, FieldValue value) {
        return readLocked(() -> {
            requireRegistered(type);
            validateField(type, fieldName);
            for (Record record : findRecords(type, fieldName, value)) {
                if (record != null) {
                    return toStoredObject(type, record);
                }
            }
            return null;
        });
    }

    @Override
    public <T> List<StoredObject<T>> findBy(ObjectType<T> type, String fieldName, FieldValue value) {
        return readLocked(() -> {
            requireRegistered(type);
            validateField(type, fieldName);
            List<StoredObject<T>> matches = new ArrayList<>();
            for (Record record : findRecords(type, fieldName, value)) {
                if (record != null) {
                    matches.add(toStoredObject(type, record));
                }
            }
            return matches;
        });
    }

    @Override
    public <T> boolean delete(ObjectType<T> type, String key) {
        return writeLocked(() -> {
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
        });
    }

    @Override
    public void close() {
        writeLocked(() -> {
            storageEngine.close();
            return null;
        });
    }

    public <T> StoredObject<T> resolveReference(ObjectType<T> type, ReferenceValue reference) {
        return readLocked(() -> {
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
            return toStoredObject(type, target);
        });
    }

    private <T> T readLocked(Supplier<T> action) {
        return action.get();
    }

    private <T> T writeLocked(Supplier<T> action) {
        writeLock.lock();
        try {
            return action.get();
        } finally {
            writeLock.unlock();
        }
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

    private void initializeMetadataSchema() {
        storageEngine.schemaRegistry().register(OBJECT_TYPE_METADATA);
        storageEngine.schemaRegistry().register(OBJECT_FIELD_METADATA);
        storageEngine.schemaRegistry().register(OBJECT_INDEX_METADATA);
    }

    private void loadPersistedGeneratedTypes() {
        Map<String, String> keyFields = new LinkedHashMap<>();
        Map<String, List<ObjectFieldDefinition>> fields = new LinkedHashMap<>();
        Map<String, List<IndexDefinition>> indexes = new LinkedHashMap<>();

        for (Record record : storageEngine.recordStore().scan(OBJECT_TYPE_METADATA)) {
            String typeName = stringField(record, "typeName");
            String keyField = stringField(record, "keyField");
            if (typeName != null && keyField != null) {
                keyFields.put(typeName, keyField);
            }
        }
        for (Record record : storageEngine.recordStore().scan(OBJECT_FIELD_METADATA)) {
            String typeName = stringField(record, "typeName");
            String fieldName = stringField(record, "fieldName");
            String valueType = stringField(record, "valueType");
            if (typeName == null || fieldName == null || valueType == null) {
                continue;
            }
            fields.computeIfAbsent(typeName, ignored -> new ArrayList<>()).add(new ObjectFieldDefinition(
                    fieldName,
                    ValueType.valueOf(valueType),
                    booleanField(record, "required"),
                    booleanField(record, "repeated"),
                    nullableStringField(record, "referenceTarget")
            ));
        }
        for (Record record : storageEngine.recordStore().scan(OBJECT_INDEX_METADATA)) {
            String typeName = stringField(record, "typeName");
            String indexName = stringField(record, "indexName");
            String indexKind = stringField(record, "indexKind");
            List<String> fieldNames = stringListField(record, "fieldNames");
            if (typeName == null || indexName == null || indexKind == null) {
                continue;
            }
            indexes.computeIfAbsent(typeName, ignored -> new ArrayList<>()).add(new IndexDefinition(
                    indexName,
                    IndexKind.valueOf(indexKind),
                    fieldNames
            ));
        }

        for (Map.Entry<String, String> entry : keyFields.entrySet()) {
            String typeName = entry.getKey();
            ObjectTypeDefinition definition = new ObjectTypeDefinition(
                    typeName,
                    entry.getValue(),
                    fields.getOrDefault(typeName, List.of()),
                    indexes.getOrDefault(typeName, List.of())
            );
            generatedDefinitions.put(typeName, definition);
        }
        for (ObjectTypeDefinition definition : generatedDefinitions.values()) {
            ObjectType<Map<String, Object>> type = GeneratedObjectTypes.create(
                    definition,
                    this::generatedType,
                    this::generatedTypeDefinition
            );
            types.put(type.name(), type);
            storageEngine.schemaRegistry().register(type.entityType());
            rebuildIndexes(type);
        }
    }

    private void persistDefinition(ObjectTypeDefinition definition) {
        try (Transaction tx = storageEngine.begin(TransactionMode.READ_WRITE)) {
            deleteDefinitionRecords(definition.name());
            RecordStore recordStore = storageEngine.recordStore();
            recordStore.create(OBJECT_TYPE_METADATA, new Record(null, OBJECT_TYPE_METADATA, Map.of(
                    "typeName", new StringValue(definition.name()),
                    "keyField", new StringValue(definition.keyField())
            )));
            for (ObjectFieldDefinition field : definition.fields()) {
                Map<String, FieldValue> fields = new LinkedHashMap<>();
                fields.put("typeName", new StringValue(definition.name()));
                fields.put("fieldName", new StringValue(field.name()));
                fields.put("valueType", new StringValue(field.type().name()));
                fields.put("required", new BooleanValue(field.required()));
                fields.put("repeated", new BooleanValue(field.repeated()));
                if (field.referenceTarget() != null) {
                    fields.put("referenceTarget", new StringValue(field.referenceTarget()));
                }
                recordStore.create(OBJECT_FIELD_METADATA, new Record(null, OBJECT_FIELD_METADATA, fields));
            }
            for (IndexDefinition index : definition.indexes()) {
                List<FieldValue> fieldNames = new ArrayList<>();
                for (String fieldName : index.fields()) {
                    fieldNames.add(new StringValue(fieldName));
                }
                recordStore.create(OBJECT_INDEX_METADATA, new Record(null, OBJECT_INDEX_METADATA, Map.of(
                        "typeName", new StringValue(definition.name()),
                        "indexName", new StringValue(index.name()),
                        "indexKind", new StringValue(index.kind().name()),
                        "fieldNames", new ListValue(fieldNames)
                )));
            }
            tx.commit();
        }
    }

    private void deleteDefinitionRecords(String typeName) {
        deleteMetadataRecords(OBJECT_TYPE_METADATA, typeName);
        deleteMetadataRecords(OBJECT_FIELD_METADATA, typeName);
        deleteMetadataRecords(OBJECT_INDEX_METADATA, typeName);
    }

    private void deleteMetadataRecords(EntityType metadataType, String typeName) {
        List<RecordId> ids = new ArrayList<>();
        for (Record record : storageEngine.recordStore().scan(metadataType)) {
            if (Objects.equals(typeName, nullableStringField(record, "typeName"))) {
                ids.add(record.id());
            }
        }
        for (RecordId id : ids) {
            storageEngine.recordStore().delete(id);
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
        if (indexedFields(type).contains(fieldName)) {
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

    private String nullableStringField(Record record, String field) {
        FieldValue value = record.fields().get(field);
        if (value == null || value == NullValue.INSTANCE) {
            return null;
        }
        return value instanceof StringValue stringValue ? stringValue.value() : null;
    }

    private boolean booleanField(Record record, String field) {
        FieldValue value = record.fields().get(field);
        return value instanceof BooleanValue booleanValue && booleanValue.value();
    }

    private List<String> stringListField(Record record, String field) {
        FieldValue value = record.fields().get(field);
        if (!(value instanceof ListValue listValue)) {
            return List.of();
        }
        List<String> values = new ArrayList<>();
        for (FieldValue item : listValue.values()) {
            if (item instanceof StringValue stringValue) {
                values.add(stringValue.value());
            }
        }
        return values;
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
