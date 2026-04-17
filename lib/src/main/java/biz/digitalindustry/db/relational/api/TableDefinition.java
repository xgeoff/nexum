package biz.digitalindustry.db.relational.api;

import biz.digitalindustry.db.schema.EntityType;
import biz.digitalindustry.db.schema.FieldDefinition;
import biz.digitalindustry.db.schema.IndexDefinition;
import biz.digitalindustry.db.schema.IndexKind;
import biz.digitalindustry.db.schema.SchemaDefinition;
import biz.digitalindustry.db.schema.SchemaKind;
import biz.digitalindustry.db.schema.ValueType;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;

public final class TableDefinition {
    public static final String PRIMARY_KEY_FIELD = "rowKey";
    public static final String PRIMARY_KEY_COLUMN_INDEX_PREFIX = "__table_pk__";

    private final String name;
    private final String primaryKeyColumn;
    private final List<FieldDefinition> columns;
    private final List<IndexDefinition> indexes;
    private final EntityType entityType;
    private final SchemaDefinition schemaDefinition;

    public TableDefinition(String name, String primaryKeyColumn, List<FieldDefinition> columns, List<IndexDefinition> indexes) {
        this.name = name;
        this.primaryKeyColumn = primaryKeyColumn;
        this.columns = List.copyOf(columns);
        this.indexes = List.copyOf(indexes);
        validateColumns();
        this.entityType = new EntityType(name, mergeColumns(), mergeIndexes());
        this.schemaDefinition = SchemaDefinition.of(entityType, SchemaKind.RELATIONAL);
    }

    public String name() {
        return name;
    }

    public String primaryKeyColumn() {
        return primaryKeyColumn;
    }

    public List<FieldDefinition> columns() {
        return columns;
    }

    public List<IndexDefinition> indexes() {
        return indexes;
    }

    public EntityType entityType() {
        return entityType;
    }

    public SchemaDefinition schemaDefinition() {
        return schemaDefinition;
    }

    public static boolean isRelationalEntityType(EntityType entityType) {
        return isRelationalSchemaDefinition(SchemaDefinition.of(entityType, SchemaKind.RELATIONAL));
    }

    public static boolean isRelationalSchemaDefinition(SchemaDefinition schemaDefinition) {
        if (schemaDefinition.kind() != SchemaKind.RELATIONAL) {
            return false;
        }
        boolean hasPrimaryKeyField = false;
        boolean hasPrimaryIndex = false;
        for (FieldDefinition field : schemaDefinition.fields()) {
            if (PRIMARY_KEY_FIELD.equals(field.name())) {
                hasPrimaryKeyField = true;
                break;
            }
        }
        for (IndexDefinition index : schemaDefinition.indexes()) {
            if (index.kind() == IndexKind.PRIMARY
                    && index.fields().size() == 1
                    && PRIMARY_KEY_FIELD.equals(index.fields().get(0))) {
                hasPrimaryIndex = true;
                break;
            }
        }
        return hasPrimaryKeyField && hasPrimaryIndex;
    }

    public static TableDefinition fromEntityType(EntityType entityType) {
        return fromSchemaDefinition(SchemaDefinition.of(entityType, SchemaKind.RELATIONAL));
    }

    public static TableDefinition fromSchemaDefinition(SchemaDefinition schemaDefinition) {
        if (!isRelationalSchemaDefinition(schemaDefinition)) {
            throw new IllegalArgumentException("Schema '" + schemaDefinition.name() + "' is not a relational table definition");
        }

        List<FieldDefinition> columns = new ArrayList<>();
        String primaryKeyColumn = null;
        for (FieldDefinition field : schemaDefinition.fields()) {
            if (PRIMARY_KEY_FIELD.equals(field.name())) {
                continue;
            }
            columns.add(field);
        }
        for (IndexDefinition index : schemaDefinition.indexes()) {
            if (index.name().startsWith(PRIMARY_KEY_COLUMN_INDEX_PREFIX)
                    && index.fields().size() == 1) {
                primaryKeyColumn = index.fields().get(0);
                break;
            }
        }
        if (primaryKeyColumn == null) {
            throw new IllegalArgumentException("Unable to infer primary key column for relational schema '" + schemaDefinition.name() + "'");
        }

        List<IndexDefinition> indexes = new ArrayList<>();
        for (IndexDefinition index : schemaDefinition.indexes()) {
            if (index.kind() == IndexKind.PRIMARY
                    && index.fields().size() == 1
                    && PRIMARY_KEY_FIELD.equals(index.fields().get(0))) {
                continue;
            }
            if (index.name().startsWith(PRIMARY_KEY_COLUMN_INDEX_PREFIX)) {
                continue;
            }
            indexes.add(index);
        }
        return new TableDefinition(schemaDefinition.name(), primaryKeyColumn, columns, indexes);
    }

    private void validateColumns() {
        boolean foundPrimaryKey = false;
        for (FieldDefinition column : columns) {
            if (PRIMARY_KEY_FIELD.equals(column.name())) {
                throw new IllegalArgumentException("Column name '" + PRIMARY_KEY_FIELD + "' is reserved for relational identity");
            }
            if (column.name().equals(primaryKeyColumn)) {
                foundPrimaryKey = true;
            }
        }
        if (!foundPrimaryKey) {
            throw new IllegalArgumentException("Primary key column '" + primaryKeyColumn + "' is not present in table '" + name + "'");
        }
    }

    private List<FieldDefinition> mergeColumns() {
        List<FieldDefinition> merged = new ArrayList<>();
        merged.add(new FieldDefinition(PRIMARY_KEY_FIELD, ValueType.STRING, true, false));
        merged.addAll(columns);
        return List.copyOf(merged);
    }

    private List<IndexDefinition> mergeIndexes() {
        List<IndexDefinition> merged = new ArrayList<>();
        merged.add(new IndexDefinition("row_key_pk", IndexKind.PRIMARY, List.of(PRIMARY_KEY_FIELD)));
        merged.add(new IndexDefinition(PRIMARY_KEY_COLUMN_INDEX_PREFIX + primaryKeyColumn, IndexKind.UNIQUE, List.of(primaryKeyColumn)));
        LinkedHashSet<IndexDefinition> deduped = new LinkedHashSet<>(indexes);
        merged.addAll(deduped);
        return List.copyOf(merged);
    }
}
