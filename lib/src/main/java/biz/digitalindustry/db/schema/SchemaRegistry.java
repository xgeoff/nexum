package biz.digitalindustry.db.schema;

import java.util.Collection;

public interface SchemaRegistry {
    SchemaDefinition schema(String name);
    void register(SchemaDefinition schema);
    Collection<SchemaDefinition> schemas();

    default EntityType entityType(String name) {
        SchemaDefinition schema = schema(name);
        return schema != null ? schema.entityType() : null;
    }

    default void register(EntityType type) {
        register(SchemaDefinition.of(type, SchemaKind.CORE));
    }

    default Collection<EntityType> entityTypes() {
        return schemas().stream()
                .map(SchemaDefinition::entityType)
                .toList();
    }
}
