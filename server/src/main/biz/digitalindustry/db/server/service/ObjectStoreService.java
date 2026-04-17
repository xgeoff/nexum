package biz.digitalindustry.db.server.service;

import biz.digitalindustry.db.engine.api.StorageConfig;
import biz.digitalindustry.db.model.FieldValue;
import biz.digitalindustry.db.object.api.GeneratedObjectTypes;
import biz.digitalindustry.db.object.api.ObjectFieldDefinition;
import biz.digitalindustry.db.object.api.ObjectStoreContext;
import biz.digitalindustry.db.object.api.ObjectTypeDefinition;
import biz.digitalindustry.db.object.api.ObjectType;
import biz.digitalindustry.db.object.api.StoredObject;
import biz.digitalindustry.db.object.runtime.NativeObjectStore;
import biz.digitalindustry.db.model.ReferenceValue;
import biz.digitalindustry.db.schema.IndexDefinition;
import biz.digitalindustry.db.schema.IndexKind;
import biz.digitalindustry.db.schema.ValueType;
import io.micronaut.context.annotation.Property;
import io.micronaut.context.annotation.Value;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Singleton;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

@Singleton
public class ObjectStoreService implements AutoCloseable {
    private final Path dbPath;
    private final boolean memoryOnly;
    private final long pageSize;
    private final long maxWalBytes;
    private final ReentrantLock writeLock = new ReentrantLock();
    private final Map<String, RegisteredType> types = new ConcurrentHashMap<>();
    private NativeObjectStore delegate;

    public ObjectStoreService(
            @Property(name = "object.db.path", defaultValue = "./data/nexum-object.dbs") String configuredPath,
            @Value("${object.db.mode:file}") String configuredMode,
            @Value("${object.db.page-size:8192}") long configuredPageSize,
            @Value("${object.db.max-wal-bytes:536870912}") long configuredMaxWalBytes
    ) throws IOException {
        memoryOnly = "memory".equalsIgnoreCase(configuredMode);
        pageSize = configuredPageSize;
        maxWalBytes = configuredMaxWalBytes;
        if (memoryOnly) {
            dbPath = Path.of("memory:nexum-object");
        } else {
            dbPath = Path.of(configuredPath);
            Path parent = dbPath.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
        }
        openDatabase();
    }

    public RegisteredTypeInfo registerType(String typeName, Map<String, Object> definition) {
        return writeLocked(() -> {
            if (types.containsKey(typeName)) {
                throw new IllegalArgumentException("Object type '" + typeName + "' is already registered");
            }
            RegisteredType registeredType = buildRegisteredType(typeName, definition);
            types.put(typeName, registeredType);
            return new RegisteredTypeInfo(registeredType.name, registeredType.keyField);
        });
    }

    public StoredDocument put(String typeName, String key, Map<String, Object> document) {
        return writeLocked(() -> {
            RegisteredType type = requireType(typeName);
            Map<String, Object> normalized = normalizeDocument(type, key, document, true);
            StoredObject<Map<String, Object>> stored = delegate.save(type.objectType, normalized);
            return toStoredDocument(stored);
        });
    }

    public StoredDocument get(String typeName, String key) {
        return readLocked(() -> {
            RegisteredType type = requireType(typeName);
            return toStoredDocument(delegate.get(type.objectType, key));
        });
    }

    public List<StoredDocument> scan(String typeName) {
        return readLocked(() -> {
            RegisteredType type = requireType(typeName);
            List<StoredDocument> results = new ArrayList<>();
            for (StoredObject<Map<String, Object>> stored : delegate.getAll(type.objectType)) {
                results.add(toStoredDocument(stored));
            }
            return results;
        });
    }

    public List<StoredDocument> findEq(String typeName, String fieldName, Object rawValue) {
        return readLocked(() -> {
            RegisteredType type = requireType(typeName);
            ObjectFieldDefinition field = requireField(type, fieldName);
            FieldValue value = GeneratedObjectTypes.toFieldValue(
                    field,
                    rawValue,
                    new ServerObjectStoreContext(),
                    delegate::generatedType,
                    delegate::generatedTypeDefinition
            );
            List<StoredDocument> results = new ArrayList<>();
            for (StoredObject<Map<String, Object>> stored : delegate.findBy(type.objectType, fieldName, value)) {
                results.add(toStoredDocument(stored));
            }
            return results;
        });
    }

    public DeleteResult delete(String typeName, String key) {
        return writeLocked(() -> {
            RegisteredType type = requireType(typeName);
            boolean deleted = delegate.delete(type.objectType, key);
            return new DeleteResult(key, deleted);
        });
    }

    public StoredDocument patch(String typeName, String key, Map<String, Object> patch) {
        return writeLocked(() -> {
            RegisteredType type = requireType(typeName);
            StoredObject<Map<String, Object>> existing = delegate.get(type.objectType, key);
            if (existing == null) {
                return null;
            }

            Map<String, Object> updated = new LinkedHashMap<>(existing.value());
            applyPatch(type, updated, patch);
            Map<String, Object> normalized = normalizeDocument(type, key, updated, false);
            StoredObject<Map<String, Object>> stored = delegate.save(type.objectType, normalized);
            return toStoredDocument(stored);
        });
    }

    public void reset() throws IOException {
        writeLock.lock();
        try {
            closeDatabase();
            types.clear();
            if (!memoryOnly) {
                Files.deleteIfExists(dbPath);
                Files.deleteIfExists(Path.of(dbPath + ".records"));
                Files.deleteIfExists(Path.of(dbPath + ".wal"));
                Files.deleteIfExists(Path.of(dbPath + ".indexes"));
            }
            openDatabase();
        } finally {
            writeLock.unlock();
        }
    }

    @PreDestroy
    @Override
    public void close() {
        writeLock.lock();
        try {
            closeDatabase();
            types.clear();
        } finally {
            writeLock.unlock();
        }
    }

    private void applyPatch(RegisteredType type, Map<String, Object> updated, Map<String, Object> patch) {
        rejectUnknownKeys(patch, Set.of("set", "inc"), "patch");
        Object setValue = patch.get("set");
        Object incValue = patch.get("inc");
        if (!(setValue instanceof Map<?, ?>) && !(incValue instanceof Map<?, ?>)) {
            throw new IllegalArgumentException("patch must contain at least one of 'set' or 'inc'");
        }

        if (setValue instanceof Map<?, ?> rawSet) {
            Map<String, Object> set = stringObjectMap(rawSet, "patch.set");
            for (Map.Entry<String, Object> entry : set.entrySet()) {
                ObjectFieldDefinition field = requireField(type, entry.getKey());
                if (field.name().equals(type.keyField)) {
                    throw new IllegalArgumentException("The key field '" + type.keyField + "' is immutable through patch");
                }
                updated.put(field.name(), entry.getValue());
            }
        }

        if (incValue instanceof Map<?, ?> rawInc) {
            Map<String, Object> inc = stringObjectMap(rawInc, "patch.inc");
            for (Map.Entry<String, Object> entry : inc.entrySet()) {
                ObjectFieldDefinition field = requireField(type, entry.getKey());
                if (field.name().equals(type.keyField)) {
                    throw new IllegalArgumentException("The key field '" + type.keyField + "' is immutable through patch");
                }
                if (field.type() != ValueType.LONG && field.type() != ValueType.DOUBLE) {
                    throw new IllegalArgumentException("inc is only supported for long and double fields");
                }
                Object current = updated.get(field.name());
                if (!(current instanceof Number currentNumber)) {
                    throw new IllegalArgumentException("Field '" + field.name() + "' must already contain a numeric value to use inc");
                }
                Object deltaValue = entry.getValue();
                if (!(deltaValue instanceof Number deltaNumber)) {
                    throw new IllegalArgumentException("inc requires numeric delta values");
                }
                if (field.type() == ValueType.LONG) {
                    updated.put(field.name(), Math.addExact(currentNumber.longValue(), deltaNumber.longValue()));
                } else {
                    updated.put(field.name(), currentNumber.doubleValue() + deltaNumber.doubleValue());
                }
            }
        }
    }

    private RegisteredType buildRegisteredType(String typeName, Map<String, Object> definition) {
        rejectUnknownKeys(definition, Set.of("keyField", "fields", "indexes"), "definition");
        String keyField = requireString(definition, "keyField");
        Map<String, Object> fieldsDefinition = requireObject(definition, "fields");
        List<ObjectFieldDefinition> fieldDefinitions = new ArrayList<>();
        Map<String, ObjectFieldDefinition> fields = new LinkedHashMap<>();

        for (Map.Entry<String, Object> entry : fieldsDefinition.entrySet()) {
            String fieldName = entry.getKey();
            Map<String, Object> fieldDefinition = requireObject(entry.getValue(), "fields." + fieldName);
            rejectUnknownKeys(fieldDefinition, Set.of("type", "required", "repeated", "target"), "fields." + fieldName);
            ValueType valueType = parseValueType(requireString(fieldDefinition, "type"));
            if (valueType == ValueType.LIST || valueType == ValueType.ANY) {
                throw new IllegalArgumentException("Field type '" + valueType.name().toLowerCase() + "' is not yet supported by the server object API");
            }
            boolean required = optionalBoolean(fieldDefinition, "required", false);
            boolean repeated = optionalBoolean(fieldDefinition, "repeated", false);
            String targetType = null;
            if (valueType == ValueType.REFERENCE) {
                targetType = requireString(fieldDefinition, "target");
            } else if (fieldDefinition.containsKey("target")) {
                throw new IllegalArgumentException("Field '" + fieldName + "' may only specify target when type is reference");
            }

            ObjectFieldDefinition field = new ObjectFieldDefinition(fieldName, valueType, required, repeated, targetType);
            fields.put(fieldName, field);
            fieldDefinitions.add(field);
        }

        ObjectFieldDefinition keyFieldSpec = fields.get(keyField);
        if (keyFieldSpec == null) {
            throw new IllegalArgumentException("keyField '" + keyField + "' must reference a defined field");
        }
        if (keyFieldSpec.type() != ValueType.STRING) {
            throw new IllegalArgumentException("keyField '" + keyField + "' must be declared as type string");
        }
        if (!keyFieldSpec.required()) {
            throw new IllegalArgumentException("keyField '" + keyField + "' must be marked required");
        }

        List<IndexDefinition> indexDefinitions = new ArrayList<>();
        Object indexesObject = definition.get("indexes");
        if (indexesObject != null) {
            if (!(indexesObject instanceof List<?> rawIndexes)) {
                throw new IllegalArgumentException("indexes must be an array");
            }
            Set<String> indexNames = new LinkedHashSet<>();
            for (Object rawIndex : rawIndexes) {
                Map<String, Object> indexDefinition = requireObject(rawIndex, "indexes[]");
                rejectUnknownKeys(indexDefinition, Set.of("name", "kind", "fields"), "indexes[]");
                String indexName = requireString(indexDefinition, "name");
                if (!indexNames.add(indexName)) {
                    throw new IllegalArgumentException("Duplicate index name '" + indexName + "'");
                }
                IndexKind kind = parseIndexKind(requireString(indexDefinition, "kind"));
                List<String> indexFields = requireStringList(indexDefinition, "fields");
                if (indexFields.isEmpty()) {
                    throw new IllegalArgumentException("Index '" + indexName + "' must define at least one field");
                }
                for (String fieldName : indexFields) {
                    if (!fields.containsKey(fieldName)) {
                        throw new IllegalArgumentException("Index '" + indexName + "' references unknown field '" + fieldName + "'");
                    }
                }
                indexDefinitions.add(new IndexDefinition(indexName, kind, indexFields));
            }
        }

        ObjectTypeDefinition objectTypeDefinition = new ObjectTypeDefinition(typeName, keyField, fieldDefinitions, indexDefinitions);
        ObjectType<Map<String, Object>> objectType = delegate.registerGeneratedType(objectTypeDefinition);
        RegisteredType registeredType = new RegisteredType(typeName, keyField, fields, indexDefinitions, objectTypeDefinition, objectType);
        return registeredType;
    }

    private Map<String, Object> normalizeDocument(RegisteredType type, String key, Map<String, Object> document, boolean keyFieldOptional) {
        Map<String, Object> normalized = GeneratedObjectTypes.normalizeDocument(
                type.definition,
                key,
                document,
                delegate::generatedTypeDefinition
        );
        if (!keyFieldOptional) {
            for (ObjectFieldDefinition field : type.definition.fields()) {
                if (field.required() && !normalized.containsKey(field.name())) {
                    throw new IllegalArgumentException("Missing required field '" + field.name() + "'");
                }
            }
        }
        return normalized;
    }

    private StoredDocument toStoredDocument(StoredObject<Map<String, Object>> stored) {
        if (stored == null) {
            return null;
        }
        return new StoredDocument(stored.key(), new LinkedHashMap<>(stored.value()));
    }

    private RegisteredType requireType(String typeName) {
        RegisteredType registeredType = types.get(typeName);
        if (registeredType == null) {
            throw new IllegalArgumentException("Unknown object type '" + typeName + "'");
        }
        return registeredType;
    }

    private ObjectFieldDefinition requireField(RegisteredType type, String fieldName) {
        ObjectFieldDefinition field = type.fields.get(fieldName);
        if (field == null) {
            throw new IllegalArgumentException("Unknown field '" + fieldName + "' for object type '" + type.name + "'");
        }
        return field;
    }

    private void openDatabase() {
        StorageConfig config = memoryOnly
                ? StorageConfig.memory(dbPath.toString(), pageSize, maxWalBytes)
                : StorageConfig.file(dbPath.toString(), pageSize, maxWalBytes, true);
        delegate = new NativeObjectStore(config);
        types.clear();
        for (ObjectTypeDefinition definition : delegate.generatedTypeDefinitions()) {
            Map<String, ObjectFieldDefinition> fields = new LinkedHashMap<>();
            for (ObjectFieldDefinition field : definition.fields()) {
                fields.put(field.name(), field);
            }
            types.put(definition.name(), new RegisteredType(
                    definition.name(),
                    definition.keyField(),
                    fields,
                    definition.indexes(),
                    definition,
                    delegate.generatedType(definition.name())
            ));
        }
    }

    private void closeDatabase() {
        if (delegate != null) {
            delegate.close();
            delegate = null;
        }
    }

    private static String requireString(Map<String, Object> map, String key) {
        return requireStringValue(map.get(key), key);
    }

    private static String requireStringValue(Object value, String field) {
        if (value instanceof String string && !string.isBlank()) {
            return string;
        }
        throw new IllegalArgumentException("'" + field + "' must be a non-empty string");
    }

    private static boolean requireBoolean(Object value, String field) {
        if (value instanceof Boolean bool) {
            return bool;
        }
        throw new IllegalArgumentException("'" + field + "' must be a boolean");
    }

    private static long requireLong(Object value, String field) {
        if (value instanceof Number number) {
            double asDouble = number.doubleValue();
            long asLong = number.longValue();
            if (Double.compare(asDouble, (double) asLong) == 0) {
                return asLong;
            }
        }
        throw new IllegalArgumentException("'" + field + "' must be a whole number");
    }

    private static double requireDouble(Object value, String field) {
        if (value instanceof Number number) {
            return number.doubleValue();
        }
        throw new IllegalArgumentException("'" + field + "' must be numeric");
    }

    private static boolean optionalBoolean(Map<String, Object> map, String key, boolean defaultValue) {
        Object value = map.get(key);
        return value == null ? defaultValue : requireBoolean(value, key);
    }

    private static ValueType parseValueType(String value) {
        try {
            return ValueType.valueOf(value.trim().toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Unsupported field type '" + value + "'");
        }
    }

    private static IndexKind parseIndexKind(String value) {
        try {
            return IndexKind.valueOf(value.trim().toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Unsupported index kind '" + value + "'");
        }
    }

    private static List<String> requireStringList(Map<String, Object> map, String key) {
        Object value = map.get(key);
        if (!(value instanceof List<?> rawList)) {
            throw new IllegalArgumentException("'" + key + "' must be an array of strings");
        }
        List<String> values = new ArrayList<>();
        for (Object element : rawList) {
            if (!(element instanceof String string) || string.isBlank()) {
                throw new IllegalArgumentException("'" + key + "' must contain only non-empty strings");
            }
            values.add(string);
        }
        return values;
    }

    private static Map<String, Object> requireObject(Map<String, Object> map, String key) {
        return requireObject(map.get(key), key);
    }

    private static Map<String, Object> requireObject(Object value, String field) {
        if (value instanceof Map<?, ?> rawMap) {
            return stringObjectMap(rawMap, field);
        }
        throw new IllegalArgumentException("'" + field + "' must be an object");
    }

    private static Map<String, Object> stringObjectMap(Map<?, ?> rawMap, String field) {
        Map<String, Object> map = new LinkedHashMap<>();
        for (Map.Entry<?, ?> entry : rawMap.entrySet()) {
            if (!(entry.getKey() instanceof String key)) {
                throw new IllegalArgumentException("'" + field + "' must use string keys");
            }
            map.put(key, entry.getValue());
        }
        return map;
    }

    private static void rejectUnknownKeys(Map<String, ?> map, Set<String> allowed, String scope) {
        for (String key : map.keySet()) {
            if (!allowed.contains(key)) {
                throw new IllegalArgumentException("Unknown field '" + key + "' in " + scope);
            }
        }
    }

    private <T> T readLocked(Supplier<T> action) {
        return action.get();
    }

    private <T> T writeLocked(Supplier<T> action) {
        writeLock.lock();
        try {
            return action.get();
        } finally {
            writeLock.unlock();
        }
    }

    public record RegisteredTypeInfo(String type, String keyField) {
    }

    public record StoredDocument(String key, Map<String, Object> document) {
    }

    public record DeleteResult(String key, boolean deleted) {
    }

    private static final class RegisteredType {
        private final String name;
        private final String keyField;
        private final Map<String, ObjectFieldDefinition> fields;
        private final List<IndexDefinition> indexes;
        private final ObjectTypeDefinition definition;
        private final ObjectType<Map<String, Object>> objectType;

        private RegisteredType(
                String name,
                String keyField,
                Map<String, ObjectFieldDefinition> fields,
                List<IndexDefinition> indexes,
                ObjectTypeDefinition definition,
                ObjectType<Map<String, Object>> objectType
        ) {
            this.name = name;
            this.keyField = keyField;
            this.fields = fields;
            this.indexes = indexes;
            this.definition = definition;
            this.objectType = objectType;
        }
    }

    private final class ServerObjectStoreContext implements ObjectStoreContext {
        @Override
        public ReferenceValue reference(ObjectType<?> type, String key) {
            RegisteredType registeredType = types.values().stream()
                    .filter(candidate -> candidate.objectType == type)
                    .findFirst()
                    .orElseThrow(() -> new IllegalArgumentException("Unknown object type reference"));
            StoredObject<Map<String, Object>> target = delegate.get(registeredType.objectType, key);
            if (target == null) {
                throw new IllegalArgumentException("Referenced object '" + registeredType.name + ":" + key + "' does not exist");
            }
            return new ReferenceValue(target.id());
        }

        @Override
        public <T> T resolve(ObjectType<T> type, ReferenceValue reference) {
            StoredObject<T> stored = delegate.resolveReference(type, reference);
            return stored == null ? null : stored.value();
        }
    }
}
