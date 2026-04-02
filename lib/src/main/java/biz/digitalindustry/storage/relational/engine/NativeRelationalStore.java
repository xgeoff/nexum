package biz.digitalindustry.storage.relational.engine;

import biz.digitalindustry.storage.relational.api.RelationalStore;
import biz.digitalindustry.storage.relational.api.Row;
import biz.digitalindustry.storage.relational.api.TableDefinition;
import biz.digitalindustry.storage.api.StorageConfig;
import biz.digitalindustry.storage.engine.NativeStorageEngine;
import biz.digitalindustry.storage.model.BooleanValue;
import biz.digitalindustry.storage.model.DoubleValue;
import biz.digitalindustry.storage.model.FieldValue;
import biz.digitalindustry.storage.model.LongValue;
import biz.digitalindustry.storage.model.Record;
import biz.digitalindustry.storage.model.RecordId;
import biz.digitalindustry.storage.model.StringValue;
import biz.digitalindustry.storage.index.OrderedRangeIndex;
import biz.digitalindustry.storage.schema.EntityType;
import biz.digitalindustry.storage.store.RecordStore;
import biz.digitalindustry.storage.tx.Transaction;
import biz.digitalindustry.storage.tx.TransactionMode;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

public class NativeRelationalStore implements RelationalStore {
    private static final int DEFAULT_PAGE_SIZE = 8192;
    private static final String ORDERED_NAMESPACE_PREFIX = "ordered:";

    private final NativeStorageEngine storageEngine;
    private final Map<String, TableDefinition> tables = new ConcurrentHashMap<>();
    private final ReentrantLock writeLock = new ReentrantLock();

    public NativeRelationalStore(String path) {
        this(StorageConfig.file(path, DEFAULT_PAGE_SIZE));
    }

    public NativeRelationalStore(StorageConfig config) {
        this.storageEngine = new NativeStorageEngine();
        this.storageEngine.open(config);
        restoreTablesFromSchemaRegistry();
    }

    public static NativeRelationalStore fileBacked(String path) {
        return new NativeRelationalStore(StorageConfig.file(path, DEFAULT_PAGE_SIZE));
    }

    public static NativeRelationalStore memoryOnly() {
        return new NativeRelationalStore(StorageConfig.memory("memory:nexum-relational", DEFAULT_PAGE_SIZE, StorageConfig.DEFAULT_MAX_WAL_BYTES));
    }

    @Override
    public void registerTable(TableDefinition table) {
        writeLocked(() -> {
            tables.put(table.name(), table);
            storageEngine.schemaRegistry().register(table.entityType());
            rebuildIndexes(table);
            return null;
        });
    }

    @Override
    public TableDefinition table(String name) {
        return readLocked(() -> tables.get(name));
    }

    @Override
    public Row upsert(TableDefinition table, Row row) {
        return writeLocked(() -> {
            requireRegistered(table);
            validateRow(table, row);
            try (Transaction tx = storageEngine.begin(TransactionMode.READ_WRITE)) {
                Record existing = findRecordByPrimaryKey(table, row.primaryKey());
                RecordStore recordStore = storageEngine.recordStore();
                Map<String, FieldValue> storedValues = withPrimaryKey(table, row);
                Record stored;
                if (existing == null) {
                    stored = recordStore.create(table.entityType(), new Record(null, table.entityType(), storedValues));
                } else {
                    removeFromIndexes(table, existing);
                    stored = new Record(existing.id(), table.entityType(), storedValues);
                    recordStore.update(stored);
                }
                addToIndexes(table, stored);
                tx.commit();
                return toRow(table, stored);
            }
        });
    }

    @Override
    public Row get(TableDefinition table, String primaryKey) {
        return readLocked(() -> {
            requireRegistered(table);
            return toRow(table, findRecordByPrimaryKey(table, primaryKey));
        });
    }

    @Override
    public List<Row> getAll(TableDefinition table) {
        return readLocked(() -> {
            requireRegistered(table);
            List<Row> rows = new ArrayList<>();
            for (Record record : storageEngine.recordStore().scan(table.entityType())) {
                rows.add(toRow(table, record));
            }
            return rows;
        });
    }

    @Override
    public Row findOneBy(TableDefinition table, String columnName, FieldValue value) {
        return readLocked(() -> {
            requireRegistered(table);
            validateColumn(table, columnName);
            for (Row row : findRows(table, columnName, value)) {
                if (row != null) {
                    return row;
                }
            }
            return null;
        });
    }

    @Override
    public List<Row> findBy(TableDefinition table, String columnName, FieldValue value) {
        return readLocked(() -> {
            requireRegistered(table);
            validateColumn(table, columnName);
            List<Row> matches = new ArrayList<>();
            for (Row row : findRows(table, columnName, value)) {
                if (row != null) {
                    matches.add(row);
                }
            }
            return matches;
        });
    }

    @Override
    public List<Row> findRangeBy(TableDefinition table, String columnName, FieldValue fromInclusive, FieldValue toInclusive) {
        return readLocked(() -> {
            requireRegistered(table);
            validateColumn(table, columnName);
            List<Row> matches = new ArrayList<>();
            if (orderedColumns(table).contains(columnName)) {
                for (Object id : storageEngine.orderedIndexRange(orderedNamespace(table), columnName, fromInclusive, toInclusive)) {
                    matches.add(toRow(table, storageEngine.recordStore().get((RecordId) id)));
                }
                return matches;
            }
            for (Record record : storageEngine.recordStore().scan(table.entityType())) {
                FieldValue actual = record.fields().get(columnName);
                if (actual != null
                        && OrderedRangeIndex.compare(actual, fromInclusive) >= 0
                        && OrderedRangeIndex.compare(actual, toInclusive) <= 0) {
                    matches.add(toRow(table, record));
                }
            }
            return matches;
        });
    }

    @Override
    public boolean delete(TableDefinition table, String primaryKey) {
        return writeLocked(() -> {
            requireRegistered(table);
            try (Transaction tx = storageEngine.begin(TransactionMode.READ_WRITE)) {
                Record existing = findRecordByPrimaryKey(table, primaryKey);
                if (existing == null) {
                    tx.rollback();
                    return false;
                }
                removeFromIndexes(table, existing);
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

    private void requireRegistered(TableDefinition table) {
        if (tables.get(table.name()) != table) {
            throw new IllegalStateException("Table '" + table.name() + "' is not registered in this store");
        }
    }

    private void restoreTablesFromSchemaRegistry() {
        tables.clear();
        for (EntityType entityType : storageEngine.schemaRegistry().entityTypes()) {
            if (TableDefinition.isRelationalEntityType(entityType)) {
                TableDefinition table = TableDefinition.fromEntityType(entityType);
                tables.put(table.name(), table);
                rebuildIndexes(table);
            }
        }
    }

    private void validateRow(TableDefinition table, Row row) {
        if (row.primaryKey() == null || row.primaryKey().isBlank()) {
            throw new IllegalArgumentException("Row primary key is required for table '" + table.name() + "'");
        }
        for (var column : table.columns()) {
            if (column.required() && !row.values().containsKey(column.name())) {
                throw new IllegalArgumentException("Missing required column '" + column.name() + "' for table '" + table.name() + "'");
            }
        }
        FieldValue primaryKeyValue = row.values().get(table.primaryKeyColumn());
        if (primaryKeyValue == null) {
            throw new IllegalArgumentException("Missing primary key column '" + table.primaryKeyColumn() + "' for table '" + table.name() + "'");
        }
        String normalizedPrimaryKey = primaryKeyString(primaryKeyValue);
        if (!row.primaryKey().equals(normalizedPrimaryKey)) {
            throw new IllegalArgumentException("Row primary key '" + row.primaryKey() + "' does not match column '" + table.primaryKeyColumn() + "' value '" + normalizedPrimaryKey + "'");
        }
    }

    private void validateColumn(TableDefinition table, String columnName) {
        if (TableDefinition.PRIMARY_KEY_FIELD.equals(columnName)) {
            return;
        }
        for (var column : table.columns()) {
            if (column.name().equals(columnName)) {
                return;
            }
        }
        throw new IllegalArgumentException("Unknown column '" + columnName + "' for table '" + table.name() + "'");
    }

    private Map<String, FieldValue> withPrimaryKey(TableDefinition table, Row row) {
        Map<String, FieldValue> stored = new LinkedHashMap<>();
        stored.put(TableDefinition.PRIMARY_KEY_FIELD, new StringValue(row.primaryKey()));
        stored.putAll(row.values());
        return stored;
    }

    private Record findRecordByPrimaryKey(TableDefinition table, String primaryKey) {
        if (indexedColumns(table).contains(TableDefinition.PRIMARY_KEY_FIELD)) {
            for (Object id : storageEngine.exactIndexFind(indexNamespace(table), TableDefinition.PRIMARY_KEY_FIELD, new StringValue(primaryKey))) {
                return storageEngine.recordStore().get((RecordId) id);
            }
        }
        for (Record record : storageEngine.recordStore().scan(table.entityType())) {
            if (primaryKey.equals(stringField(record, TableDefinition.PRIMARY_KEY_FIELD))) {
                return record;
            }
        }
        return null;
    }

    private List<Row> findRows(TableDefinition table, String columnName, FieldValue value) {
        if (indexedColumns(table).contains(columnName)) {
            List<Row> rows = new ArrayList<>();
            for (Object id : storageEngine.exactIndexFind(indexNamespace(table), columnName, value)) {
                rows.add(toRow(table, storageEngine.recordStore().get((RecordId) id)));
            }
            return rows;
        }

        List<Row> scanned = new ArrayList<>();
        for (Record record : storageEngine.recordStore().scan(table.entityType())) {
            if (matchesField(record, columnName, value)) {
                scanned.add(toRow(table, record));
            }
        }
        return scanned;
    }

    private boolean matchesField(Record record, String fieldName, FieldValue expected) {
        FieldValue actual = record.fields().get(fieldName);
        return actual == null ? expected == null : actual.equals(expected);
    }

    private String stringField(Record record, String fieldName) {
        FieldValue value = record.fields().get(fieldName);
        return value instanceof StringValue stringValue ? stringValue.value() : null;
    }

    private Row toRow(TableDefinition table, Record record) {
        if (record == null) {
            return null;
        }
        String primaryKey = stringField(record, TableDefinition.PRIMARY_KEY_FIELD);
        Map<String, FieldValue> values = new LinkedHashMap<>(record.fields());
        values.remove(TableDefinition.PRIMARY_KEY_FIELD);
        return new Row(primaryKey, values);
    }

    private void rebuildIndexes(TableDefinition table) {
        storageEngine.ensureExactMatchNamespace(
                indexNamespace(table),
                indexedColumns(table),
                table.entityType(),
                Record::id
        );
        storageEngine.ensureOrderedRangeNamespace(
                orderedNamespace(table),
                orderedColumns(table),
                table.entityType(),
                Record::id
        );
    }

    private List<String> indexedColumns(TableDefinition table) {
        List<String> columns = new ArrayList<>();
        columns.add(TableDefinition.PRIMARY_KEY_FIELD);
        for (var index : table.indexes()) {
            if (index.fields().size() == 1) {
                String columnName = index.fields().get(0);
                if (!columns.contains(columnName)) {
                    columns.add(columnName);
                }
            }
        }
        return columns;
    }

    private void addToIndexes(TableDefinition table, Record record) {
        if (record == null) {
            return;
        }
        storageEngine.exactIndexAdd(indexNamespace(table), indexedColumns(table), record.fields(), record.id());
        for (String column : orderedColumns(table)) {
            storageEngine.orderedIndexAdd(orderedNamespace(table), orderedColumns(table), column, record.fields().get(column), record.id());
        }
    }

    private void removeFromIndexes(TableDefinition table, Record record) {
        if (record == null) {
            return;
        }
        storageEngine.exactIndexRemove(indexNamespace(table), indexedColumns(table), record.fields(), record.id());
        for (String column : orderedColumns(table)) {
            storageEngine.orderedIndexRemove(orderedNamespace(table), orderedColumns(table), column, record.fields().get(column), record.id());
        }
    }

    private String indexNamespace(TableDefinition table) {
        return "table:" + table.name();
    }

    private String orderedNamespace(TableDefinition table) {
        return ORDERED_NAMESPACE_PREFIX + table.name();
    }

    private List<String> orderedColumns(TableDefinition table) {
        List<String> columns = new ArrayList<>();
        for (var index : table.indexes()) {
            if (index.kind() == biz.digitalindustry.storage.schema.IndexKind.ORDERED_RANGE && index.fields().size() == 1) {
                String columnName = index.fields().get(0);
                if (!columns.contains(columnName)) {
                    columns.add(columnName);
                }
            }
        }
        return columns;
    }

    public static String primaryKeyString(FieldValue value) {
        if (value instanceof StringValue stringValue) {
            return stringValue.value();
        }
        if (value instanceof LongValue longValue) {
            return Long.toString(longValue.value());
        }
        if (value instanceof DoubleValue doubleValue) {
            return Double.toString(doubleValue.value());
        }
        if (value instanceof BooleanValue booleanValue) {
            return Boolean.toString(booleanValue.value());
        }
        throw new IllegalArgumentException("Unsupported primary key field value: " + value);
    }
}
