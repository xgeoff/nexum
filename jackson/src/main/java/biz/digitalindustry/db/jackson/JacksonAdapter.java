package biz.digitalindustry.db.jackson;

import biz.digitalindustry.db.object.api.ObjectTypeDefinition;
import biz.digitalindustry.db.object.api.StoredObject;
import biz.digitalindustry.db.object.runtime.NativeObjectStore;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public final class JacksonAdapter implements AutoCloseable {
    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {
    };

    private final NativeObjectStore store;
    private final ObjectMapper objectMapper;
    private final Map<Class<?>, ObjectTypeDefinition> registrations = new LinkedHashMap<>();

    public JacksonAdapter(String path) {
        this(NativeObjectStore.fileBacked(path), new ObjectMapper());
    }

    public JacksonAdapter(NativeObjectStore store, ObjectMapper objectMapper) {
        this.store = store;
        this.objectMapper = objectMapper;
    }

    public static JacksonAdapter fileBacked(String path) {
        return new JacksonAdapter(path);
    }

    public static JacksonAdapter memoryOnly() {
        return new JacksonAdapter(NativeObjectStore.memoryOnly(), new ObjectMapper());
    }

    public <T> ObjectTypeDefinition register(Class<T> dtoType, String keyField) {
        return register(dtoType, keyField, ignored -> {
        });
    }

    public <T> ObjectTypeDefinition register(Class<T> dtoType, String keyField, Consumer<JacksonObjectTypes.Builder> customizer) {
        JacksonObjectTypes.Builder builder = new JacksonObjectTypes(objectMapper).defineWithMapper(dtoType);
        builder.key(keyField);
        customizer.accept(builder);
        ObjectTypeDefinition definition = builder.build();
        store.registerGeneratedType(definition);
        registrations.put(dtoType, definition);
        return definition;
    }

    public void register(ObjectTypeDefinition definition) {
        store.registerGeneratedType(definition);
    }

    public <T> StoredObject<T> save(T dto) {
        @SuppressWarnings("unchecked")
        Class<T> dtoType = (Class<T>) dto.getClass();
        ObjectTypeDefinition definition = requireDefinition(dtoType);
        Map<String, Object> document = objectMapper.convertValue(dto, MAP_TYPE);
        StoredObject<Map<String, Object>> stored = store.save(store.generatedType(definition.name()), document);
        T value = objectMapper.convertValue(stored.value(), dtoType);
        return new StoredObject<>(stored.id(), stored.key(), value);
    }

    public <T> StoredObject<T> get(Class<T> dtoType, String key) {
        ObjectTypeDefinition definition = requireDefinition(dtoType);
        StoredObject<Map<String, Object>> stored = store.get(store.generatedType(definition.name()), key);
        if (stored == null) {
            return null;
        }
        T value = objectMapper.convertValue(stored.value(), dtoType);
        return new StoredObject<>(stored.id(), stored.key(), value);
    }

    public <T> List<StoredObject<T>> getAll(Class<T> dtoType) {
        ObjectTypeDefinition definition = requireDefinition(dtoType);
        return store.getAll(store.generatedType(definition.name())).stream()
                .map(stored -> new StoredObject<>(
                        stored.id(),
                        stored.key(),
                        objectMapper.convertValue(stored.value(), dtoType)
                ))
                .toList();
    }

    public <T> boolean delete(Class<T> dtoType, String key) {
        ObjectTypeDefinition definition = requireDefinition(dtoType);
        return store.delete(store.generatedType(definition.name()), key);
    }

    public NativeObjectStore store() {
        return store;
    }

    private <T> ObjectTypeDefinition requireDefinition(Class<T> dtoType) {
        ObjectTypeDefinition definition = registrations.get(dtoType);
        if (definition == null) {
            throw new IllegalStateException("Jackson DTO '" + dtoType.getSimpleName() + "' is not registered");
        }
        return definition;
    }

    @Override
    public void close() {
        store.close();
    }
}
