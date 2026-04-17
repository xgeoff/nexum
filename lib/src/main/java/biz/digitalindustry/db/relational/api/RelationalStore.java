package biz.digitalindustry.db.relational.api;

import biz.digitalindustry.db.model.FieldValue;
import biz.digitalindustry.db.model.Vector;

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

    List<VectorRowMatch> findNearestBy(TableDefinition table, String columnName, Vector query, int limit);

    boolean delete(TableDefinition table, String primaryKey);

    @Override
    void close();
}
