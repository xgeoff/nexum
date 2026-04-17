package biz.digitalindustry.db.vector.runtime;

import biz.digitalindustry.db.engine.NativeStorageEngine;
import biz.digitalindustry.db.engine.api.StorageConfig;
import biz.digitalindustry.db.engine.record.RecordStore;
import biz.digitalindustry.db.engine.tx.Transaction;
import biz.digitalindustry.db.engine.tx.TransactionMode;
import biz.digitalindustry.db.index.VectorMatch;
import biz.digitalindustry.db.model.FieldValue;
import biz.digitalindustry.db.model.Record;
import biz.digitalindustry.db.model.RecordId;
import biz.digitalindustry.db.model.StringValue;
import biz.digitalindustry.db.model.Vector;
import biz.digitalindustry.db.model.VectorValue;
import biz.digitalindustry.db.schema.FieldDefinition;
import biz.digitalindustry.db.schema.IndexDefinition;
import biz.digitalindustry.db.schema.IndexKind;
import biz.digitalindustry.db.schema.SchemaDefinition;
import biz.digitalindustry.db.vector.Distances;
import biz.digitalindustry.db.vector.VectorIndexDefinition;
import biz.digitalindustry.db.vector.api.VectorCollectionDefinition;
import biz.digitalindustry.db.vector.api.VectorDocument;
import biz.digitalindustry.db.vector.api.VectorDocumentMatch;
import biz.digitalindustry.db.vector.api.VectorStore;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

public class NativeVectorStore implements VectorStore {
    private static final int DEFAULT_PAGE_SIZE = 8192;
    private static final String ORDERED_NAMESPACE_PREFIX = "ordered:";
    private static final String VECTOR_NAMESPACE_PREFIX = "vector:";

    private final NativeStorageEngine storageEngine;
    private final Map<String, VectorCollectionDefinition> collections = new ConcurrentHashMap<>();
    private final ReentrantLock writeLock = new ReentrantLock();

    public NativeVectorStore(String path) {
        this(StorageConfig.file(path, DEFAULT_PAGE_SIZE));
    }

    public NativeVectorStore(StorageConfig config) {
        this.storageEngine = new NativeStorageEngine();
        this.storageEngine.open(config);
        restoreCollectionsFromSchemaRegistry();
    }

    public static NativeVectorStore fileBacked(String path) {
        return new NativeVectorStore(StorageConfig.file(path, DEFAULT_PAGE_SIZE));
    }

    public static NativeVectorStore memoryOnly() {
        return new NativeVectorStore(StorageConfig.memory("memory:nexum-vector", DEFAULT_PAGE_SIZE, StorageConfig.DEFAULT_MAX_WAL_BYTES));
    }

    @Override
    public void registerCollection(VectorCollectionDefinition collection) {
        writeLocked(() -> {
            collections.put(collection.name(), collection);
            storageEngine.schemaRegistry().register(collection.schemaDefinition());
            rebuildIndexes(collection);
            return null;
        });
    }

    @Override
    public VectorCollectionDefinition collection(String name) {
        return readLocked(() -> collections.get(name));
    }

    @Override
    public VectorDocument upsert(VectorCollectionDefinition collection, VectorDocument document) {
        return writeLocked(() -> {
            requireRegistered(collection);
            validateDocument(collection, document);
            try (Transaction tx = storageEngine.begin(TransactionMode.READ_WRITE)) {
                Record existing = findRecordByKey(collection, document.key());
                RecordStore recordStore = storageEngine.recordStore();
                Map<String, FieldValue> storedValues = withInternalKey(collection, document);
                Record stored;
                if (existing == null) {
                    stored = recordStore.create(collection.entityType(), new Record(null, collection.entityType(), storedValues));
                } else {
                    removeFromIndexes(collection, existing);
                    stored = new Record(existing.id(), collection.entityType(), storedValues);
                    recordStore.update(stored);
                }
                addToIndexes(collection, stored);
                tx.commit();
                return toDocument(collection, stored);
            }
        });
    }

    @Override
    public VectorDocument get(VectorCollectionDefinition collection, String key) {
        return readLocked(() -> {
            requireRegistered(collection);
            return toDocument(collection, findRecordByKey(collection, key));
        });
    }

    @Override
    public List<VectorDocument> getAll(VectorCollectionDefinition collection) {
        return readLocked(() -> {
            requireRegistered(collection);
            List<VectorDocument> documents = new ArrayList<>();
            for (Record record : storageEngine.recordStore().scan(collection.entityType())) {
                documents.add(toDocument(collection, record));
            }
            return documents;
        });
    }

    @Override
    public List<VectorDocument> findBy(VectorCollectionDefinition collection, String fieldName, FieldValue value) {
        return readLocked(() -> {
            requireRegistered(collection);
            validateField(collection, fieldName);
            List<VectorDocument> matches = new ArrayList<>();
            for (Record record : findRecords(collection, fieldName, value)) {
                if (record != null) {
                    matches.add(toDocument(collection, record));
                }
            }
            return matches;
        });
    }

    @Override
    public List<VectorDocumentMatch> nearest(VectorCollectionDefinition collection, Vector query, int limit) {
        return readLocked(() -> {
            requireRegistered(collection);
            if (limit <= 0) {
                return List.of();
            }
            requireVectorDimension(collection, query.dimension());
            List<VectorDocumentMatch> matches = new ArrayList<>();
            for (VectorMatch<Object> match : storageEngine.vectorNearest(vectorNamespace(collection), collection.vectorField(), query, limit)) {
                VectorDocument document = toDocument(collection, storageEngine.recordStore().get((RecordId) match.id()));
                if (document != null) {
                    matches.add(new VectorDocumentMatch(document, match.distance()));
                }
            }
            if (!matches.isEmpty()) {
                return matches;
            }
            for (Record record : storageEngine.recordStore().scan(collection.entityType())) {
                FieldValue value = record.fields().get(collection.vectorField());
                if (value instanceof VectorValue vectorValue) {
                    matches.add(new VectorDocumentMatch(
                            toDocument(collection, record),
                            Distances.resolve(collection.distanceMetric()).distance(query, vectorValue.vector())
                    ));
                }
            }
            matches.sort(java.util.Comparator.comparing(VectorDocumentMatch::distance));
            return List.copyOf(matches.subList(0, Math.min(limit, matches.size())));
        });
    }

    @Override
    public boolean delete(VectorCollectionDefinition collection, String key) {
        return writeLocked(() -> {
            requireRegistered(collection);
            try (Transaction tx = storageEngine.begin(TransactionMode.READ_WRITE)) {
                Record existing = findRecordByKey(collection, key);
                if (existing == null) {
                    tx.rollback();
                    return false;
                }
                removeFromIndexes(collection, existing);
                boolean deleted = storageEngine.recordStore().delete(existing.id());
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

    private void restoreCollectionsFromSchemaRegistry() {
        collections.clear();
        for (SchemaDefinition schemaDefinition : storageEngine.schemaRegistry().schemas()) {
            if (VectorCollectionDefinition.isVectorSchemaDefinition(schemaDefinition)) {
                VectorCollectionDefinition collection = VectorCollectionDefinition.fromSchemaDefinition(schemaDefinition);
                collections.put(collection.name(), collection);
                rebuildIndexes(collection);
            }
        }
    }

    private void requireRegistered(VectorCollectionDefinition collection) {
        if (collections.get(collection.name()) != collection) {
            throw new IllegalStateException("Vector collection '" + collection.name() + "' is not registered in this store");
        }
    }

    private void validateDocument(VectorCollectionDefinition collection, VectorDocument document) {
        if (document.key() == null || document.key().isBlank()) {
            throw new IllegalArgumentException("Vector document key is required for collection '" + collection.name() + "'");
        }
        for (FieldDefinition field : collection.fields()) {
            if (field.required() && !document.values().containsKey(field.name())) {
                throw new IllegalArgumentException("Missing required field '" + field.name() + "' for collection '" + collection.name() + "'");
            }
            FieldValue value = document.values().get(field.name());
            if (value != null) {
                validateFieldValue(field, value, collection);
            }
        }
        FieldValue keyValue = document.values().get(collection.keyField());
        if (keyValue == null) {
            throw new IllegalArgumentException("Missing key field '" + collection.keyField() + "' for collection '" + collection.name() + "'");
        }
        String normalizedKey = keyValue instanceof StringValue stringValue ? stringValue.value() : null;
        if (!document.key().equals(normalizedKey)) {
            throw new IllegalArgumentException("Vector document key '" + document.key() + "' does not match field '" + collection.keyField() + "' value '" + normalizedKey + "'");
        }
    }

    private void validateField(VectorCollectionDefinition collection, String fieldName) {
        if (VectorCollectionDefinition.INTERNAL_KEY_FIELD.equals(fieldName)) {
            return;
        }
        for (FieldDefinition field : collection.fields()) {
            if (field.name().equals(fieldName)) {
                return;
            }
        }
        throw new IllegalArgumentException("Unknown field '" + fieldName + "' for collection '" + collection.name() + "'");
    }

    private Map<String, FieldValue> withInternalKey(VectorCollectionDefinition collection, VectorDocument document) {
        Map<String, FieldValue> stored = new LinkedHashMap<>();
        stored.put(VectorCollectionDefinition.INTERNAL_KEY_FIELD, new StringValue(document.key()));
        stored.putAll(document.values());
        return stored;
    }

    private Record findRecordByKey(VectorCollectionDefinition collection, String key) {
        for (Object id : storageEngine.exactIndexFind(indexNamespace(collection), VectorCollectionDefinition.INTERNAL_KEY_FIELD, new StringValue(key))) {
            return storageEngine.recordStore().get((RecordId) id);
        }
        for (Record record : storageEngine.recordStore().scan(collection.entityType())) {
            if (key.equals(stringField(record, VectorCollectionDefinition.INTERNAL_KEY_FIELD))) {
                return record;
            }
        }
        return null;
    }

    private List<Record> findRecords(VectorCollectionDefinition collection, String fieldName, FieldValue value) {
        List<Record> matches = new ArrayList<>();
        for (Object id : storageEngine.exactIndexFind(indexNamespace(collection), fieldName, value)) {
            Record record = storageEngine.recordStore().get((RecordId) id);
            if (record != null) {
                matches.add(record);
            }
        }
        if (!matches.isEmpty()) {
            return matches;
        }
        for (Record record : storageEngine.recordStore().scan(collection.entityType())) {
            FieldValue actual = record.fields().get(fieldName);
            if (value.equals(actual)) {
                matches.add(record);
            }
        }
        return matches;
    }

    private VectorDocument toDocument(VectorCollectionDefinition collection, Record record) {
        if (record == null) {
            return null;
        }
        Map<String, FieldValue> values = new LinkedHashMap<>(record.fields());
        values.remove(VectorCollectionDefinition.INTERNAL_KEY_FIELD);
        return new VectorDocument(stringField(record, VectorCollectionDefinition.INTERNAL_KEY_FIELD), values);
    }

    private void rebuildIndexes(VectorCollectionDefinition collection) {
        storageEngine.ensureExactMatchNamespace(indexNamespace(collection), exactIndexedFields(collection), collection.entityType(), Record::id);
        if (!orderedIndexedFields(collection).isEmpty()) {
            storageEngine.ensureOrderedRangeNamespace(orderedNamespace(collection), orderedIndexedFields(collection), collection.entityType(), Record::id);
        }
        storageEngine.ensureVectorNamespace(vectorNamespace(collection), vectorDefinitions(collection), collection.entityType(), Record::id);
        for (Record record : storageEngine.recordStore().scan(collection.entityType())) {
            addToIndexes(collection, record);
        }
    }

    private void addToIndexes(VectorCollectionDefinition collection, Record record) {
        if (!exactIndexedFields(collection).isEmpty()) {
            storageEngine.exactIndexAdd(indexNamespace(collection), exactIndexedFields(collection), record.fields(), record.id());
        }
        for (IndexDefinition index : collection.entityType().indexes()) {
            List<String> fields = index.fields();
            if (fields.size() != 1) {
                continue;
            }
            String fieldName = fields.get(0);
            FieldValue value = record.fields().get(fieldName);
            if (value == null) {
                continue;
            }
            if (index.kind() == IndexKind.ORDERED_RANGE) {
                storageEngine.orderedIndexAdd(orderedNamespace(collection), orderedIndexedFields(collection), fieldName, value, record.id());
            } else if (index.kind() == IndexKind.VECTOR && value instanceof VectorValue vectorValue) {
                requireVectorDimension(collection, vectorValue.vector().dimension());
                storageEngine.vectorIndexAdd(
                        vectorNamespace(collection),
                        vectorDefinitions(collection),
                    fieldName,
                    vectorValue.vector(),
                    record.id(),
                    collection.distanceMetric()
                );
            }
        }
    }

    private void removeFromIndexes(VectorCollectionDefinition collection, Record record) {
        if (!exactIndexedFields(collection).isEmpty()) {
            storageEngine.exactIndexRemove(indexNamespace(collection), exactIndexedFields(collection), record.fields(), record.id());
        }
        for (IndexDefinition index : collection.entityType().indexes()) {
            List<String> fields = index.fields();
            if (fields.size() != 1) {
                continue;
            }
            String fieldName = fields.get(0);
            FieldValue value = record.fields().get(fieldName);
            if (value == null) {
                continue;
            }
            if (index.kind() == IndexKind.ORDERED_RANGE) {
                storageEngine.orderedIndexRemove(orderedNamespace(collection), orderedIndexedFields(collection), fieldName, value, record.id());
            } else if (index.kind() == IndexKind.VECTOR) {
                storageEngine.vectorIndexRemove(vectorNamespace(collection), vectorDefinitions(collection), fieldName, record.id(), collection.distanceMetric());
            }
        }
    }

    private void validateFieldValue(FieldDefinition field, FieldValue value, VectorCollectionDefinition collection) {
        switch (field.type()) {
            case STRING -> requireType(field, value instanceof StringValue, collection.name());
            case LONG -> requireType(field, value instanceof biz.digitalindustry.db.model.LongValue, collection.name());
            case DOUBLE -> requireType(field, value instanceof biz.digitalindustry.db.model.DoubleValue, collection.name());
            case BOOLEAN -> requireType(field, value instanceof biz.digitalindustry.db.model.BooleanValue, collection.name());
            case VECTOR -> {
                requireType(field, value instanceof VectorValue, collection.name());
                requireVectorDimension(collection, ((VectorValue) value).vector().dimension());
            }
            default -> {
            }
        }
    }

    private void requireType(FieldDefinition field, boolean valid, String collectionName) {
        if (!valid) {
            throw new IllegalArgumentException("Field '" + field.name() + "' in collection '" + collectionName + "' must use " + field.type());
        }
    }

    private void requireVectorDimension(VectorCollectionDefinition collection, int actualDimension) {
        if (actualDimension != collection.dimension()) {
            throw new IllegalArgumentException("Vector field '" + collection.vectorField() + "' in collection '" + collection.name() + "' requires dimension "
                    + collection.dimension() + " but received " + actualDimension);
        }
    }

    private String indexNamespace(VectorCollectionDefinition collection) {
        return collection.name();
    }

    private String orderedNamespace(VectorCollectionDefinition collection) {
        return ORDERED_NAMESPACE_PREFIX + collection.name();
    }

    private String vectorNamespace(VectorCollectionDefinition collection) {
        return VECTOR_NAMESPACE_PREFIX + collection.name();
    }

    private String stringField(Record record, String fieldName) {
        FieldValue value = record.fields().get(fieldName);
        return value instanceof StringValue stringValue ? stringValue.value() : null;
    }

    private List<String> exactIndexedFields(VectorCollectionDefinition collection) {
        List<String> fields = new ArrayList<>();
        for (IndexDefinition index : collection.entityType().indexes()) {
            if (index.kind() == IndexKind.ORDERED_RANGE || index.kind() == IndexKind.VECTOR) {
                continue;
            }
            fields.addAll(index.fields());
        }
        return List.copyOf(fields);
    }

    private List<String> orderedIndexedFields(VectorCollectionDefinition collection) {
        List<String> fields = new ArrayList<>();
        for (IndexDefinition index : collection.entityType().indexes()) {
            if (index.kind() == IndexKind.ORDERED_RANGE) {
                fields.addAll(index.fields());
            }
        }
        return List.copyOf(fields);
    }

    private List<VectorIndexDefinition> vectorDefinitions(VectorCollectionDefinition collection) {
        return List.of(new VectorIndexDefinition(collection.vectorField(), collection.dimension(), collection.distanceMetric()));
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
}
