package biz.digitalindustry.db.jackson;

import biz.digitalindustry.db.object.api.ObjectTypeDefinition;
import biz.digitalindustry.db.object.api.ObjectTypes;
import biz.digitalindustry.db.schema.ValueType;
import com.fasterxml.jackson.databind.BeanDescription;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.introspect.BeanPropertyDefinition;

import java.util.LinkedHashSet;
import java.util.Set;

public final class JacksonObjectTypes {
    private final ObjectMapper objectMapper;

    public JacksonObjectTypes(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public static Builder define(Class<?> dtoType) {
        return new Builder(new ObjectMapper(), dtoType);
    }

    public static ObjectTypeDefinition define(Class<?> dtoType, String keyField) {
        return define(dtoType).key(keyField).build();
    }

    public Builder defineWithMapper(Class<?> dtoType) {
        return new Builder(objectMapper, dtoType);
    }

    public static final class Builder {
        private final Class<?> dtoType;
        private final ObjectTypes.Builder delegate;
        private final Set<String> inferredFields = new LinkedHashSet<>();

        private Builder(ObjectMapper objectMapper, Class<?> dtoType) {
            this.dtoType = dtoType;
            this.delegate = ObjectTypes.define(dtoType.getSimpleName());
            inferFields(objectMapper, dtoType);
        }

        public Builder key(String fieldName) {
            delegate.key(fieldName);
            delegate.string(fieldName).required();
            return this;
        }

        public ObjectTypes.ScalarFieldBuilder string(String fieldName) {
            inferredFields.remove(fieldName);
            return delegate.string(fieldName);
        }

        public ObjectTypes.ScalarFieldBuilder longNumber(String fieldName) {
            inferredFields.remove(fieldName);
            return delegate.longNumber(fieldName);
        }

        public ObjectTypes.ScalarFieldBuilder doubleNumber(String fieldName) {
            inferredFields.remove(fieldName);
            return delegate.doubleNumber(fieldName);
        }

        public ObjectTypes.ScalarFieldBuilder booleanFlag(String fieldName) {
            inferredFields.remove(fieldName);
            return delegate.booleanFlag(fieldName);
        }

        public ObjectTypes.ReferenceFieldBuilder reference(String fieldName) {
            inferredFields.remove(fieldName);
            return delegate.reference(fieldName);
        }

        public ObjectTypes.IndexBuilder index(String indexName) {
            return delegate.index(indexName);
        }

        public ObjectTypes.IndexBuilder uniqueIndex(String indexName) {
            return delegate.uniqueIndex(indexName);
        }

        public ObjectTypes.IndexBuilder orderedRangeIndex(String indexName) {
            return delegate.orderedRangeIndex(indexName);
        }

        public ObjectTypeDefinition build() {
            return delegate.build();
        }

        private void inferFields(ObjectMapper objectMapper, Class<?> dtoType) {
            JavaType javaType = objectMapper.constructType(dtoType);
            BeanDescription description = objectMapper.getSerializationConfig().introspect(javaType);
            for (BeanPropertyDefinition property : description.findProperties()) {
                JavaType propertyType = property.getPrimaryType();
                if (propertyType == null) {
                    continue;
                }
                ValueType inferred = inferValueType(propertyType);
                if (inferred == null) {
                    continue;
                }
                String propertyName = property.getName();
                switch (inferred) {
                    case STRING -> delegate.string(propertyName).optional();
                    case LONG -> {
                        if (propertyType.isPrimitive()) {
                            delegate.longNumber(propertyName).required();
                        } else {
                            delegate.longNumber(propertyName).optional();
                        }
                    }
                    case DOUBLE -> {
                        if (propertyType.isPrimitive()) {
                            delegate.doubleNumber(propertyName).required();
                        } else {
                            delegate.doubleNumber(propertyName).optional();
                        }
                    }
                    case BOOLEAN -> {
                        if (propertyType.isPrimitive()) {
                            delegate.booleanFlag(propertyName).required();
                        } else {
                            delegate.booleanFlag(propertyName).optional();
                        }
                    }
                    default -> {
                    }
                }
                inferredFields.add(propertyName);
            }
        }

        private static ValueType inferValueType(JavaType type) {
            Class<?> raw = type.getRawClass();
            if (raw == String.class) {
                return ValueType.STRING;
            }
            if (raw == long.class || raw == Long.class
                    || raw == int.class || raw == Integer.class
                    || raw == short.class || raw == Short.class
                    || raw == byte.class || raw == Byte.class) {
                return ValueType.LONG;
            }
            if (raw == double.class || raw == Double.class
                    || raw == float.class || raw == Float.class) {
                return ValueType.DOUBLE;
            }
            if (raw == boolean.class || raw == Boolean.class) {
                return ValueType.BOOLEAN;
            }
            return null;
        }

        public Class<?> dtoType() {
            return dtoType;
        }
    }
}
