package biz.digitalindustry.db.schema;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

public class InMemorySchemaRegistry implements SchemaRegistry {
    private final Map<String, SchemaDefinition> types = new LinkedHashMap<>();

    public InMemorySchemaRegistry() {
    }

    public InMemorySchemaRegistry(Map<String, SchemaDefinition> initialTypes) {
        types.putAll(initialTypes);
    }

    @Override
    public synchronized SchemaDefinition schema(String name) {
        return types.get(name);
    }

    @Override
    public synchronized void register(SchemaDefinition type) {
        types.put(type.name(), type);
    }

    @Override
    public synchronized Collection<SchemaDefinition> schemas() {
        return java.util.List.copyOf(types.values());
    }

    public synchronized Map<String, SchemaDefinition> snapshot() {
        return new LinkedHashMap<>(types);
    }
}
