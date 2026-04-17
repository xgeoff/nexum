package biz.digitalindustry.db.schema;

import java.util.List;

public record SchemaDefinition(
        String name,
        SchemaKind kind,
        List<FieldDefinition> fields,
        List<IndexDefinition> indexes
) {
    public SchemaDefinition {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("Schema name must not be blank");
        }
        if (kind == null) {
            throw new IllegalArgumentException("Schema kind must not be null");
        }
        fields = List.copyOf(fields);
        indexes = List.copyOf(indexes);
    }

    public EntityType entityType() {
        return new EntityType(name, fields, indexes);
    }

    public static SchemaDefinition of(String name, SchemaKind kind, List<FieldDefinition> fields, List<IndexDefinition> indexes) {
        return new SchemaDefinition(name, kind, fields, indexes);
    }

    public static SchemaDefinition of(EntityType entityType, SchemaKind kind) {
        return new SchemaDefinition(entityType.name(), kind, entityType.fields(), entityType.indexes());
    }

    public static SchemaDefinition core(String name, List<FieldDefinition> fields, List<IndexDefinition> indexes) {
        return new SchemaDefinition(name, SchemaKind.CORE, fields, indexes);
    }
}
