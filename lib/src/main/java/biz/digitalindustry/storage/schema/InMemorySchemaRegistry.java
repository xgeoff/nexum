package biz.digitalindustry.storage.schema;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

public class InMemorySchemaRegistry implements SchemaRegistry {
    private final Map<String, EntityType> types = new LinkedHashMap<>();

    public InMemorySchemaRegistry() {
    }

    public InMemorySchemaRegistry(Map<String, EntityType> initialTypes) {
        types.putAll(initialTypes);
    }

    @Override
    public synchronized EntityType entityType(String name) {
        return types.get(name);
    }

    @Override
    public synchronized void register(EntityType type) {
        types.put(type.name(), type);
    }

    @Override
    public synchronized Collection<EntityType> entityTypes() {
        return java.util.List.copyOf(types.values());
    }

    public synchronized Map<String, EntityType> snapshot() {
        return new LinkedHashMap<>(types);
    }
}
