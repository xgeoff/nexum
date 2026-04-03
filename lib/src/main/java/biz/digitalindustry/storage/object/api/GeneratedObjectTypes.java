package biz.digitalindustry.storage.object.api;

import biz.digitalindustry.storage.model.BooleanValue;
import biz.digitalindustry.storage.model.DoubleValue;
import biz.digitalindustry.storage.model.FieldValue;
import biz.digitalindustry.storage.model.LongValue;
import biz.digitalindustry.storage.model.NullValue;
import biz.digitalindustry.storage.model.ReferenceValue;
import biz.digitalindustry.storage.model.StringValue;
import biz.digitalindustry.storage.schema.FieldDefinition;
import biz.digitalindustry.storage.schema.IndexDefinition;
import biz.digitalindustry.storage.schema.ValueType;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

public final class GeneratedObjectTypes {
    private GeneratedObjectTypes() {
    }

    public static ObjectType<Map<String, Object>> create(
            ObjectTypeDefinition definition,
            Function<String, ObjectType<Map<String, Object>>> typeResolver,
            Function<String, ObjectTypeDefinition> definitionResolver
    ) {
        validateDefinition(definition);
        List<FieldDefinition> fields = new ArrayList<>();
        for (ObjectFieldDefinition field : definition.fields()) {
            fields.add(new FieldDefinition(field.name(), field.type(), field.required(), field.repeated()));
        }
        List<IndexDefinition> indexes = List.copyOf(definition.indexes());
        return new ObjectType<>(definition.name(), fields, indexes, new GeneratedObjectCodec(definition, typeResolver, definitionResolver));
    }

    public static void validateDocumentDefinition(
            ObjectTypeDefinition definition,
            String key,
            Map<String, Object> document,
            Function<String, ObjectTypeDefinition> definitionResolver
    ) {
        normalizeDocument(definition, key, document, definitionResolver);
    }

    public static Map<String, Object> normalizeDocument(
            ObjectTypeDefinition definition,
            String key,
            Map<String, Object> document,
            Function<String, ObjectTypeDefinition> definitionResolver
    ) {
        if (document == null) {
            throw new IllegalArgumentException("document must be an object");
        }
        Map<String, ObjectFieldDefinition> fields = definition.fieldsByName();
        for (String fieldName : document.keySet()) {
            if (!fields.containsKey(fieldName)) {
                throw new IllegalArgumentException("Unknown field '" + fieldName + "' for object type '" + definition.name() + "'");
            }
        }

        Map<String, Object> normalized = new LinkedHashMap<>();
        for (ObjectFieldDefinition field : definition.fields()) {
            Object rawValue = document.get(field.name());
            if (field.name().equals(definition.keyField())) {
                if (rawValue == null) {
                    rawValue = key;
                }
                if (!Objects.equals(key, rawValue)) {
                    throw new IllegalArgumentException("The document key field '" + definition.keyField() + "' must match key '" + key + "'");
                }
            } else if (rawValue == null && field.required()) {
                throw new IllegalArgumentException("Missing required field '" + field.name() + "'");
            }

            if (rawValue == null) {
                if (field.name().equals(definition.keyField())) {
                    normalized.put(field.name(), key);
                }
                continue;
            }
            normalized.put(field.name(), canonicalizeValue(field, rawValue, definitionResolver));
        }
        return normalized;
    }

    public static Object canonicalizeValue(
            ObjectFieldDefinition field,
            Object rawValue,
            Function<String, ObjectTypeDefinition> definitionResolver
    ) {
        return switch (field.type()) {
            case STRING -> requireStringValue(rawValue, field.name());
            case LONG -> requireLong(rawValue, field.name());
            case DOUBLE -> requireDouble(rawValue, field.name());
            case BOOLEAN -> requireBoolean(rawValue, field.name());
            case REFERENCE -> canonicalReference(field, rawValue, definitionResolver);
            default -> throw new IllegalArgumentException("Unsupported field type '" + field.type().name().toLowerCase() + "'");
        };
    }

    public static FieldValue toFieldValue(
            ObjectFieldDefinition field,
            Object rawValue,
            ObjectStoreContext context,
            Function<String, ObjectType<Map<String, Object>>> typeResolver,
            Function<String, ObjectTypeDefinition> definitionResolver
    ) {
        if (rawValue == null) {
            return NullValue.INSTANCE;
        }
        return switch (field.type()) {
            case STRING -> new StringValue(requireStringValue(rawValue, field.name()));
            case LONG -> new LongValue(requireLong(rawValue, field.name()));
            case DOUBLE -> new DoubleValue(requireDouble(rawValue, field.name()));
            case BOOLEAN -> new BooleanValue(requireBoolean(rawValue, field.name()));
            case REFERENCE -> {
                Map<String, Object> reference = canonicalReference(field, rawValue, definitionResolver);
                ObjectType<Map<String, Object>> targetType = typeResolver.apply(field.referenceTarget());
                if (targetType == null) {
                    throw new IllegalArgumentException("Unknown reference target type '" + field.referenceTarget() + "'");
                }
                yield context.reference(targetType, requireStringValue(reference.get("key"), "key"));
            }
            default -> throw new IllegalArgumentException("Unsupported field type '" + field.type().name().toLowerCase() + "'");
        };
    }

    public static Object fromFieldValue(
            ObjectFieldDefinition field,
            FieldValue value,
            ObjectStoreContext context,
            Function<String, ObjectType<Map<String, Object>>> typeResolver,
            Function<String, ObjectTypeDefinition> definitionResolver
    ) {
        if (value == null || value == NullValue.INSTANCE) {
            return null;
        }
        return switch (field.type()) {
            case STRING -> ((StringValue) value).value();
            case LONG -> ((LongValue) value).value();
            case DOUBLE -> ((DoubleValue) value).value();
            case BOOLEAN -> ((BooleanValue) value).value();
            case REFERENCE -> {
                ObjectTypeDefinition targetDefinition = definitionResolver.apply(field.referenceTarget());
                ObjectType<Map<String, Object>> targetType = typeResolver.apply(field.referenceTarget());
                if (targetDefinition == null || targetType == null) {
                    throw new IllegalArgumentException("Unknown reference target type '" + field.referenceTarget() + "'");
                }
                Map<String, Object> resolved = context.resolve(targetType, (ReferenceValue) value);
                if (resolved == null) {
                    yield null;
                }
                Object key = resolved.get(targetDefinition.keyField());
                if (!(key instanceof String stringKey)) {
                    throw new IllegalArgumentException("Resolved reference does not contain key field '" + targetDefinition.keyField() + "'");
                }
                yield Map.of("type", targetDefinition.name(), "key", stringKey);
            }
            default -> throw new IllegalArgumentException("Unsupported field type '" + field.type().name().toLowerCase() + "'");
        };
    }

    public static void validateDefinition(ObjectTypeDefinition definition) {
        if (definition.name() == null || definition.name().isBlank()) {
            throw new IllegalArgumentException("Object type definition name must not be blank");
        }
        if (definition.keyField() == null || definition.keyField().isBlank()) {
            throw new IllegalArgumentException("Object type definition keyField must not be blank");
        }
        Map<String, ObjectFieldDefinition> fields = definition.fieldsByName();
        ObjectFieldDefinition keyField = fields.get(definition.keyField());
        if (keyField == null) {
            throw new IllegalArgumentException("keyField '" + definition.keyField() + "' must reference a defined field");
        }
        if (keyField.type() != ValueType.STRING) {
            throw new IllegalArgumentException("keyField '" + definition.keyField() + "' must be declared as type string");
        }
        if (!keyField.required()) {
            throw new IllegalArgumentException("keyField '" + definition.keyField() + "' must be marked required");
        }
        for (ObjectFieldDefinition field : definition.fields()) {
            if (field.type() == ValueType.REFERENCE && (field.referenceTarget() == null || field.referenceTarget().isBlank())) {
                throw new IllegalArgumentException("Reference field '" + field.name() + "' must declare a reference target");
            }
            if (field.type() != ValueType.REFERENCE && field.referenceTarget() != null) {
                throw new IllegalArgumentException("Field '" + field.name() + "' may only declare referenceTarget when type is reference");
            }
            if (field.repeated()) {
                throw new IllegalArgumentException("Repeated object fields are not yet supported");
            }
            if (field.type() == ValueType.LIST || field.type() == ValueType.ANY) {
                throw new IllegalArgumentException("Field type '" + field.type().name().toLowerCase() + "' is not yet supported");
            }
        }
    }

    private static Map<String, Object> canonicalReference(
            ObjectFieldDefinition field,
            Object rawValue,
            Function<String, ObjectTypeDefinition> definitionResolver
    ) {
        if (!(rawValue instanceof Map<?, ?> rawReference)) {
            throw new IllegalArgumentException("'" + field.name() + "' must be an object");
        }
        Map<String, Object> reference = new LinkedHashMap<>();
        for (Map.Entry<?, ?> entry : rawReference.entrySet()) {
            if (!(entry.getKey() instanceof String key)) {
                throw new IllegalArgumentException("'" + field.name() + "' must use string keys");
            }
            reference.put(key, entry.getValue());
        }
        if (!reference.keySet().equals(java.util.Set.of("type", "key"))) {
            throw new IllegalArgumentException("Reference field '" + field.name() + "' must contain only 'type' and 'key'");
        }
        String targetType = requireStringValue(reference.get("type"), "type");
        String targetKey = requireStringValue(reference.get("key"), "key");
        if (!targetType.equals(field.referenceTarget())) {
            throw new IllegalArgumentException("Reference field '" + field.name() + "' expects target type '" + field.referenceTarget() + "'");
        }
        if (definitionResolver.apply(targetType) == null) {
            throw new IllegalArgumentException("Unknown reference target type '" + targetType + "'");
        }
        return Map.of("type", targetType, "key", targetKey);
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

    private static final class GeneratedObjectCodec implements ObjectCodec<Map<String, Object>> {
        private final ObjectTypeDefinition definition;
        private final Function<String, ObjectType<Map<String, Object>>> typeResolver;
        private final Function<String, ObjectTypeDefinition> definitionResolver;

        private GeneratedObjectCodec(
                ObjectTypeDefinition definition,
                Function<String, ObjectType<Map<String, Object>>> typeResolver,
                Function<String, ObjectTypeDefinition> definitionResolver
        ) {
            this.definition = definition;
            this.typeResolver = typeResolver;
            this.definitionResolver = definitionResolver;
        }

        @Override
        public String key(Map<String, Object> object) {
            Object value = object.get(definition.keyField());
            if (!(value instanceof String key) || key.isBlank()) {
                throw new IllegalArgumentException("Document must contain key field '" + definition.keyField() + "'");
            }
            return key;
        }

        @Override
        public Map<String, FieldValue> encode(Map<String, Object> object, ObjectStoreContext context) {
            Map<String, Object> normalized = normalizeDocument(definition, key(object), object, GeneratedObjectCodec.this::definitionFor);
            Map<String, FieldValue> encoded = new LinkedHashMap<>();
            for (ObjectFieldDefinition field : definition.fields()) {
                Object value = normalized.get(field.name());
                if (value == null) {
                    continue;
                }
                encoded.put(field.name(), toFieldValue(field, value, context, typeResolver, GeneratedObjectCodec.this::definitionFor));
            }
            return encoded;
        }

        @Override
        public Map<String, Object> decode(StoredObjectView view, ObjectStoreContext context) {
            Map<String, Object> decoded = new LinkedHashMap<>();
            for (ObjectFieldDefinition field : definition.fields()) {
                Object value = fromFieldValue(field, view.fields().get(field.name()), context, typeResolver, this::definitionFor);
                if (value != null) {
                    decoded.put(field.name(), value);
                }
            }
            decoded.putIfAbsent(definition.keyField(), view.key());
            return decoded;
        }

        private ObjectTypeDefinition definitionFor(String typeName) {
            return definitionResolver.apply(typeName);
        }
    }
}
