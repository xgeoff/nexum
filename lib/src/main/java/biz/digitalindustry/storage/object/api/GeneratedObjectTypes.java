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

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
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

    public static <T> ObjectType<T> createBean(
            Class<T> beanType,
            ObjectTypeDefinition definition,
            Function<String, ObjectType<Map<String, Object>>> typeResolver,
            Function<String, ObjectTypeDefinition> definitionResolver
    ) {
        validateDefinition(definition);
        BeanBinding<T> binding = BeanBinding.create(beanType, definition);
        List<FieldDefinition> fields = new ArrayList<>();
        for (ObjectFieldDefinition field : definition.fields()) {
            fields.add(new FieldDefinition(field.name(), field.type(), field.required(), field.repeated()));
        }
        List<IndexDefinition> indexes = List.copyOf(definition.indexes());
        return new ObjectType<>(definition.name(), fields, indexes, new BeanObjectCodec<>(definition, binding, typeResolver, definitionResolver));
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

    private static Object adaptValueForProperty(
            Object value,
            Class<?> propertyType,
            ObjectFieldDefinition field
    ) {
        if (value == null) {
            if (propertyType.isPrimitive()) {
                throw new IllegalArgumentException("Field '" + field.name() + "' cannot be null for primitive property '" + propertyType.getSimpleName() + "'");
            }
            return null;
        }
        return switch (field.type()) {
            case STRING -> adaptStringProperty(value, propertyType, field.name());
            case LONG -> adaptLongProperty(value, propertyType, field.name());
            case DOUBLE -> adaptDoubleProperty(value, propertyType, field.name());
            case BOOLEAN -> adaptBooleanProperty(value, propertyType, field.name());
            case REFERENCE -> adaptReferenceProperty(value, propertyType, field.name());
            default -> throw new IllegalArgumentException("Unsupported field type '" + field.type().name().toLowerCase() + "'");
        };
    }

    private static Object adaptStringProperty(Object value, Class<?> propertyType, String fieldName) {
        String stringValue = requireStringValue(value, fieldName);
        if (propertyType == String.class || propertyType == Object.class) {
            return stringValue;
        }
        throw new IllegalArgumentException("Field '" + fieldName + "' must map to a String property");
    }

    private static Object adaptLongProperty(Object value, Class<?> propertyType, String fieldName) {
        long longValue = requireLong(value, fieldName);
        if (propertyType == Long.class || propertyType == long.class || propertyType == Object.class) {
            return longValue;
        }
        if (propertyType == Integer.class || propertyType == int.class) {
            return Math.toIntExact(longValue);
        }
        if (propertyType == Short.class || propertyType == short.class) {
            return Math.toIntExact(longValue);
        }
        if (propertyType == Byte.class || propertyType == byte.class) {
            return Math.toIntExact(longValue);
        }
        throw new IllegalArgumentException("Field '" + fieldName + "' must map to an integral numeric property");
    }

    private static Object adaptDoubleProperty(Object value, Class<?> propertyType, String fieldName) {
        double doubleValue = requireDouble(value, fieldName);
        if (propertyType == Double.class || propertyType == double.class || propertyType == Object.class) {
            return doubleValue;
        }
        if (propertyType == Float.class || propertyType == float.class) {
            return (float) doubleValue;
        }
        throw new IllegalArgumentException("Field '" + fieldName + "' must map to a floating-point numeric property");
    }

    private static Object adaptBooleanProperty(Object value, Class<?> propertyType, String fieldName) {
        boolean booleanValue = requireBoolean(value, fieldName);
        if (propertyType == Boolean.class || propertyType == boolean.class || propertyType == Object.class) {
            return booleanValue;
        }
        throw new IllegalArgumentException("Field '" + fieldName + "' must map to a boolean property");
    }

    private static Object adaptReferenceProperty(Object value, Class<?> propertyType, String fieldName) {
        if (propertyType == Object.class || propertyType.isAssignableFrom(Map.class)) {
            return value;
        }
        throw new IllegalArgumentException("Reference field '" + fieldName + "' must map to a Map-like bean property");
    }

    private static final class BeanBinding<T> {
        private final Class<T> beanType;
        private final Constructor<T> constructor;
        private final Map<String, PropertyDescriptor> propertiesByName;

        private BeanBinding(Class<T> beanType, Constructor<T> constructor, Map<String, PropertyDescriptor> propertiesByName) {
            this.beanType = beanType;
            this.constructor = constructor;
            this.propertiesByName = propertiesByName;
        }

        private static <T> BeanBinding<T> create(Class<T> beanType, ObjectTypeDefinition definition) {
            Constructor<T> constructor = defaultConstructor(beanType);
            Map<String, PropertyDescriptor> propertiesByName = introspect(beanType);

            for (ObjectFieldDefinition field : definition.fields()) {
                PropertyDescriptor descriptor = propertiesByName.get(field.name());
                if (descriptor == null || descriptor.getReadMethod() == null) {
                    throw new IllegalArgumentException("Bean type '" + beanType.getSimpleName() + "' must expose a readable property for field '" + field.name() + "'");
                }
                if (descriptor.getWriteMethod() == null) {
                    throw new IllegalArgumentException("Bean type '" + beanType.getSimpleName() + "' must expose a writable property for field '" + field.name() + "'");
                }
                validatePropertyType(descriptor, field);
            }
            return new BeanBinding<>(beanType, constructor, propertiesByName);
        }

        private Map<String, Object> extractDocument(T bean) {
            Map<String, Object> document = new LinkedHashMap<>();
            for (Map.Entry<String, PropertyDescriptor> entry : propertiesByName.entrySet()) {
                try {
                    document.put(entry.getKey(), entry.getValue().getReadMethod().invoke(bean));
                } catch (IllegalAccessException | InvocationTargetException e) {
                    throw new IllegalArgumentException("Failed to read bean property '" + entry.getKey() + "' from '" + beanType.getSimpleName() + "'", e);
                }
            }
            return document;
        }

        private T instantiate(Map<String, Object> values, ObjectTypeDefinition definition) {
            T bean;
            try {
                bean = constructor.newInstance();
            } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
                throw new IllegalArgumentException("Failed to instantiate bean type '" + beanType.getSimpleName() + "'", e);
            }
            for (ObjectFieldDefinition field : definition.fields()) {
                PropertyDescriptor descriptor = propertiesByName.get(field.name());
                Object propertyValue = adaptValueForProperty(values.get(field.name()), descriptor.getPropertyType(), field);
                try {
                    descriptor.getWriteMethod().invoke(bean, propertyValue);
                } catch (IllegalAccessException | InvocationTargetException e) {
                    throw new IllegalArgumentException("Failed to write bean property '" + field.name() + "' on '" + beanType.getSimpleName() + "'", e);
                }
            }
            return bean;
        }

        private static <T> Constructor<T> defaultConstructor(Class<T> beanType) {
            try {
                Constructor<T> constructor = beanType.getDeclaredConstructor();
                constructor.setAccessible(true);
                return constructor;
            } catch (NoSuchMethodException e) {
                throw new IllegalArgumentException("Bean type '" + beanType.getSimpleName() + "' must declare a no-arg constructor", e);
            }
        }

        private static Map<String, PropertyDescriptor> introspect(Class<?> beanType) {
            try {
                BeanInfo beanInfo = Introspector.getBeanInfo(beanType, Object.class);
                Map<String, PropertyDescriptor> descriptors = new LinkedHashMap<>();
                for (PropertyDescriptor descriptor : beanInfo.getPropertyDescriptors()) {
                    descriptors.put(descriptor.getName(), descriptor);
                }
                return descriptors;
            } catch (IntrospectionException e) {
                throw new IllegalArgumentException("Failed to inspect bean type '" + beanType.getSimpleName() + "'", e);
            }
        }

        private static void validatePropertyType(PropertyDescriptor descriptor, ObjectFieldDefinition field) {
            Class<?> propertyType = descriptor.getPropertyType();
            switch (field.type()) {
                case STRING -> {
                    if (!(propertyType == String.class || propertyType == Object.class)) {
                        throw new IllegalArgumentException("Field '" + field.name() + "' must map to a String bean property");
                    }
                }
                case LONG -> {
                    if (!(propertyType == long.class
                            || propertyType == Long.class
                            || propertyType == int.class
                            || propertyType == Integer.class
                            || propertyType == short.class
                            || propertyType == Short.class
                            || propertyType == byte.class
                            || propertyType == Byte.class
                            || propertyType == Object.class)) {
                        throw new IllegalArgumentException("Field '" + field.name() + "' must map to an integral numeric bean property");
                    }
                }
                case DOUBLE -> {
                    if (!(propertyType == double.class
                            || propertyType == Double.class
                            || propertyType == float.class
                            || propertyType == Float.class
                            || propertyType == Object.class)) {
                        throw new IllegalArgumentException("Field '" + field.name() + "' must map to a floating-point numeric bean property");
                    }
                }
                case BOOLEAN -> {
                    if (!(propertyType == boolean.class || propertyType == Boolean.class || propertyType == Object.class)) {
                        throw new IllegalArgumentException("Field '" + field.name() + "' must map to a boolean bean property");
                    }
                }
                case REFERENCE -> {
                    if (!(propertyType == Object.class || propertyType.isAssignableFrom(Map.class))) {
                        throw new IllegalArgumentException("Reference field '" + field.name() + "' must map to a Map-like bean property");
                    }
                }
                default -> throw new IllegalArgumentException("Unsupported field type '" + field.type().name().toLowerCase() + "'");
            }
            if (Collection.class.isAssignableFrom(propertyType)) {
                throw new IllegalArgumentException("Collection-valued bean properties are not supported for field '" + field.name() + "'");
            }
        }
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

    private static final class BeanObjectCodec<T> implements ObjectCodec<T> {
        private final ObjectTypeDefinition definition;
        private final BeanBinding<T> binding;
        private final Function<String, ObjectType<Map<String, Object>>> typeResolver;
        private final Function<String, ObjectTypeDefinition> definitionResolver;

        private BeanObjectCodec(
                ObjectTypeDefinition definition,
                BeanBinding<T> binding,
                Function<String, ObjectType<Map<String, Object>>> typeResolver,
                Function<String, ObjectTypeDefinition> definitionResolver
        ) {
            this.definition = definition;
            this.binding = binding;
            this.typeResolver = typeResolver;
            this.definitionResolver = definitionResolver;
        }

        @Override
        public String key(T object) {
            Map<String, Object> document = binding.extractDocument(object);
            Object value = document.get(definition.keyField());
            if (!(value instanceof String key) || key.isBlank()) {
                throw new IllegalArgumentException("Bean must expose key field '" + definition.keyField() + "' as a non-empty string");
            }
            return key;
        }

        @Override
        public Map<String, FieldValue> encode(T object, ObjectStoreContext context) {
            Map<String, Object> document = binding.extractDocument(object);
            Map<String, Object> normalized = normalizeDocument(definition, key(object), document, this::definitionFor);
            Map<String, FieldValue> encoded = new LinkedHashMap<>();
            for (ObjectFieldDefinition field : definition.fields()) {
                Object value = normalized.get(field.name());
                if (value == null) {
                    continue;
                }
                encoded.put(field.name(), toFieldValue(field, value, context, typeResolver, this::definitionFor));
            }
            return encoded;
        }

        @Override
        public T decode(StoredObjectView view, ObjectStoreContext context) {
            Map<String, Object> decoded = new LinkedHashMap<>();
            for (ObjectFieldDefinition field : definition.fields()) {
                Object value = fromFieldValue(field, view.fields().get(field.name()), context, typeResolver, this::definitionFor);
                if (value != null) {
                    decoded.put(field.name(), value);
                }
            }
            decoded.putIfAbsent(definition.keyField(), view.key());
            return binding.instantiate(decoded, definition);
        }

        private ObjectTypeDefinition definitionFor(String typeName) {
            return definitionResolver.apply(typeName);
        }
    }
}
