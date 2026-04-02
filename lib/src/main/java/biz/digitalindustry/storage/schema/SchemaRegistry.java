package biz.digitalindustry.storage.schema;

import java.util.Collection;

public interface SchemaRegistry {
    EntityType entityType(String name);
    void register(EntityType type);
    Collection<EntityType> entityTypes();
}
