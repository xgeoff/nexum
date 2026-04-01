package biz.digitalindustry.storage.relational.api;

import biz.digitalindustry.storage.model.FieldValue;

import java.util.List;

public interface RelationalStore extends AutoCloseable {
    void registerTable(TableDefinition table);

    TableDefinition table(String name);

    Row upsert(TableDefinition table, Row row);

    Row get(TableDefinition table, String primaryKey);

    List<Row> getAll(TableDefinition table);

    Row findOneBy(TableDefinition table, String columnName, FieldValue value);

    List<Row> findBy(TableDefinition table, String columnName, FieldValue value);

    List<Row> findRangeBy(TableDefinition table, String columnName, FieldValue fromInclusive, FieldValue toInclusive);

    boolean delete(TableDefinition table, String primaryKey);

    @Override
    void close();
}
