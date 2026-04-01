package biz.digitalindustry.relational.engine;

import biz.digitalindustry.relational.api.RelationalStore;
import biz.digitalindustry.relational.api.Row;
import biz.digitalindustry.relational.api.TableDefinition;
import biz.digitalindustry.storage.api.StorageConfig;
import biz.digitalindustry.storage.engine.ExactMatchIndexManager;
import biz.digitalindustry.storage.engine.NativeStorageEngine;
import biz.digitalindustry.storage.engine.OrderedRangeIndexManager;
import biz.digitalindustry.storage.model.BooleanValue;
import biz.digitalindustry.storage.model.DoubleValue;
import biz.digitalindustry.storage.model.FieldValue;
import biz.digitalindustry.storage.model.LongValue;
import biz.digitalindustry.storage.model.Record;
import biz.digitalindustry.storage.model.StringValue;
import biz.digitalindustry.storage.index.OrderedRangeIndex;
import biz.digitalindustry.storage.store.RecordStore;
import biz.digitalindustry.storage.tx.Transaction;
import biz.digitalindustry.storage.tx.TransactionMode;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class NativeRelationalStore implements RelationalStore {
    private static final int DEFAULT_PAGE_SIZE = 8192;
    private static final String ORDERED_NAMESPACE_PREFIX = "ordered:";

    private final NativeStorageEngine storageEngine;
    private final Map<String, TableDefinition> tables = new LinkedHashMap<>();

    public NativeRelationalStore(String path) {
        this.storageEngine = new NativeStorageEngine();
        this.storageEngine.open(new StorageConfig(path, DEFAULT_PAGE_SIZE));
    }

    @Override
    public synchronized void registerTable(TableDefinition table) {
        tables.put(table.name(), table);
        storageEngine.schemaRegistry().register(table.entityType());
        rebuildIndexes(table);
    }

    @Override
    public synchronized Row upsert(TableDefinition table, Row row) {
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
                stored = new Record(existing.id(), table.entityType(), storedValues);
                recordStore.update(stored);
                removeFromIndexes(table, toRow(table, existing));
            }
            addToIndexes(table, toRow(table, stored));
            tx.commit();
            return toRow(table, stored);
        }
    }

    @Override
    public synchronized Row get(TableDefinition table, String primaryKey) {
        requireRegistered(table);
        return toRow(table, findRecordByPrimaryKey(table, primaryKey));
    }

    @Override
    public synchronized List<Row> getAll(TableDefinition table) {
        requireRegistered(table);
        List<Row> rows = new ArrayList<>();
        for (Record record : storageEngine.recordStore().scan(table.entityType())) {
            rows.add(toRow(table, record));
        }
        return rows;
    }

    @Override
    public synchronized Row findOneBy(TableDefinition table, String columnName, FieldValue value) {
        requireRegistered(table);
        validateColumn(table, columnName);
        for (Row row : findRows(table, columnName, value)) {
            if (row != null) {
                return row;
            }
        }
        return null;
    }

    @Override
    public synchronized List<Row> findBy(TableDefinition table, String columnName, FieldValue value) {
        requireRegistered(table);
        validateColumn(table, columnName);
        List<Row> matches = new ArrayList<>();
        for (Row row : findRows(table, columnName, value)) {
            if (row != null) {
                matches.add(row);
            }
        }
        return matches;
    }

    @Override
    public synchronized List<Row> findRangeBy(TableDefinition table, String columnName, FieldValue fromInclusive, FieldValue toInclusive) {
        requireRegistered(table);
        validateColumn(table, columnName);
        List<Row> matches = new ArrayList<>();
        if (orderedIndexManager().hasField(orderedNamespace(table), columnName)) {
            for (Object primaryKey : storageEngine.orderedIndexRange(orderedNamespace(table), columnName, fromInclusive, toInclusive)) {
                matches.add(toRow(table, findRecordByPrimaryKey(table, (String) primaryKey)));
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
    }

    @Override
    public synchronized boolean delete(TableDefinition table, String primaryKey) {
        requireRegistered(table);
        try (Transaction tx = storageEngine.begin(TransactionMode.READ_WRITE)) {
            Record existing = findRecordByPrimaryKey(table, primaryKey);
            if (existing == null) {
                tx.rollback();
                return false;
            }
            boolean deleted = storageEngine.recordStore().delete(existing.id());
            removeFromIndexes(table, toRow(table, existing));
            tx.commit();
            return deleted;
        }
    }

    @Override
    public synchronized void close() {
        storageEngine.close();
    }

    private void requireRegistered(TableDefinition table) {
        if (tables.get(table.name()) != table) {
            throw new IllegalStateException("Table '" + table.name() + "' is not registered in this store");
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
        for (Record record : storageEngine.recordStore().scan(table.entityType())) {
            if (primaryKey.equals(stringField(record, TableDefinition.PRIMARY_KEY_FIELD))) {
                return record;
            }
        }
        return null;
    }

    private List<Row> findRows(TableDefinition table, String columnName, FieldValue value) {
        if (indexManager().hasField(indexNamespace(table), columnName)) {
            List<Row> rows = new ArrayList<>();
            for (Object primaryKey : storageEngine.exactIndexFind(indexNamespace(table), columnName, value)) {
                rows.add(toRow(table, findRecordByPrimaryKey(table, (String) primaryKey)));
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
                record -> stringField(record, TableDefinition.PRIMARY_KEY_FIELD)
        );
        storageEngine.ensureOrderedRangeNamespace(
                orderedNamespace(table),
                orderedColumns(table),
                table.entityType(),
                record -> stringField(record, TableDefinition.PRIMARY_KEY_FIELD)
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

    private void addToIndexes(TableDefinition table, Row row) {
        if (row == null) {
            return;
        }
        storageEngine.exactIndexAdd(indexNamespace(table), indexedColumns(table), indexableValues(row), row.primaryKey());
        for (String column : orderedColumns(table)) {
            storageEngine.orderedIndexAdd(orderedNamespace(table), orderedColumns(table), column, row.values().get(column), row.primaryKey());
        }
    }

    private void removeFromIndexes(TableDefinition table, Row row) {
        if (row == null) {
            return;
        }
        storageEngine.exactIndexRemove(indexNamespace(table), indexedColumns(table), indexableValues(row), row.primaryKey());
        for (String column : orderedColumns(table)) {
            storageEngine.orderedIndexRemove(orderedNamespace(table), orderedColumns(table), column, row.values().get(column), row.primaryKey());
        }
    }

    private Map<String, FieldValue> indexableValues(Row row) {
        Map<String, FieldValue> values = new LinkedHashMap<>(row.values());
        values.put(TableDefinition.PRIMARY_KEY_FIELD, new StringValue(row.primaryKey()));
        return values;
    }

    private ExactMatchIndexManager indexManager() {
        return storageEngine.indexManager();
    }

    private OrderedRangeIndexManager orderedIndexManager() {
        return storageEngine.orderedIndexManager();
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
