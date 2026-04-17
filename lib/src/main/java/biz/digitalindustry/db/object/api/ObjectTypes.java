package biz.digitalindustry.db.object.api;

import biz.digitalindustry.db.schema.IndexDefinition;
import biz.digitalindustry.db.schema.IndexKind;
import biz.digitalindustry.db.schema.ValueType;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class ObjectTypes {
    private ObjectTypes() {
    }

    public static Builder define(String name) {
        return new Builder(name);
    }

    public static BeanBuilder define(Class<?> beanType) {
        return new BeanBuilder(beanType);
    }

    public static class Builder {
        private final String name;
        private final Map<String, ObjectFieldDefinition> fields = new LinkedHashMap<>();
        private final List<IndexDefinition> indexes = new ArrayList<>();
        private String keyField;

        protected Builder(String name) {
            this.name = name;
        }

        public Builder key(String fieldName) {
            this.keyField = fieldName;
            return this;
        }

        public ScalarFieldBuilder string(String fieldName) {
            return new ScalarFieldBuilder(this, fieldName, ValueType.STRING);
        }

        public ScalarFieldBuilder longNumber(String fieldName) {
            return new ScalarFieldBuilder(this, fieldName, ValueType.LONG);
        }

        public ScalarFieldBuilder doubleNumber(String fieldName) {
            return new ScalarFieldBuilder(this, fieldName, ValueType.DOUBLE);
        }

        public ScalarFieldBuilder booleanFlag(String fieldName) {
            return new ScalarFieldBuilder(this, fieldName, ValueType.BOOLEAN);
        }

        public ReferenceFieldBuilder reference(String fieldName) {
            return new ReferenceFieldBuilder(this, fieldName);
        }

        public IndexBuilder index(String indexName) {
            return new IndexBuilder(this, indexName, IndexKind.NON_UNIQUE);
        }

        public IndexBuilder uniqueIndex(String indexName) {
            return new IndexBuilder(this, indexName, IndexKind.UNIQUE);
        }

        public IndexBuilder orderedRangeIndex(String indexName) {
            return new IndexBuilder(this, indexName, IndexKind.ORDERED_RANGE);
        }

        public ObjectTypeDefinition build() {
            return new ObjectTypeDefinition(name, keyField, new ArrayList<>(fields.values()), indexes);
        }

        protected Builder addField(ObjectFieldDefinition field) {
            fields.put(field.name(), field);
            return this;
        }

        protected Builder addIndex(IndexDefinition index) {
            indexes.add(index);
            return this;
        }

        protected Map<String, ObjectFieldDefinition> fields() {
            return fields;
        }
    }

    public static final class BeanBuilder extends Builder {
        private BeanBuilder(Class<?> beanType) {
            super(beanType.getSimpleName());
            for (PropertyDescriptor descriptor : beanProperties(beanType)) {
                ObjectFieldDefinition inferred = inferField(descriptor);
                if (inferred != null) {
                    addField(inferred);
                }
            }
        }

        @Override
        public BeanBuilder key(String fieldName) {
            super.key(fieldName);
            ObjectFieldDefinition inferred = fields().get(fieldName);
            if (inferred != null && inferred.type() == ValueType.STRING) {
                addField(ObjectFieldDefinition.required(fieldName, ValueType.STRING));
            }
            return this;
        }

        @Override
        public ScalarFieldBuilder string(String fieldName) {
            return super.string(fieldName);
        }

        @Override
        public ScalarFieldBuilder longNumber(String fieldName) {
            return super.longNumber(fieldName);
        }

        @Override
        public ScalarFieldBuilder doubleNumber(String fieldName) {
            return super.doubleNumber(fieldName);
        }

        @Override
        public ScalarFieldBuilder booleanFlag(String fieldName) {
            return super.booleanFlag(fieldName);
        }

        @Override
        public ReferenceFieldBuilder reference(String fieldName) {
            return super.reference(fieldName);
        }

        @Override
        public IndexBuilder index(String indexName) {
            return super.index(indexName);
        }

        @Override
        public IndexBuilder uniqueIndex(String indexName) {
            return super.uniqueIndex(indexName);
        }

        @Override
        public IndexBuilder orderedRangeIndex(String indexName) {
            return super.orderedRangeIndex(indexName);
        }
    }

    public static final class ScalarFieldBuilder {
        private final Builder parent;
        private final String fieldName;
        private final ValueType type;

        private ScalarFieldBuilder(Builder parent, String fieldName, ValueType type) {
            this.parent = parent;
            this.fieldName = fieldName;
            this.type = type;
        }

        public Builder required() {
            return parent.addField(new ObjectFieldDefinition(fieldName, type, true, false, null));
        }

        public Builder optional() {
            return parent.addField(new ObjectFieldDefinition(fieldName, type, false, false, null));
        }
    }

    public static final class ReferenceFieldBuilder {
        private final Builder parent;
        private final String fieldName;
        private String targetType;

        private ReferenceFieldBuilder(Builder parent, String fieldName) {
            this.parent = parent;
            this.fieldName = fieldName;
        }

        public ReferenceFieldBuilder to(String targetType) {
            this.targetType = targetType;
            return this;
        }

        public Builder required() {
            return parent.addField(new ObjectFieldDefinition(fieldName, ValueType.REFERENCE, true, false, targetType));
        }

        public Builder optional() {
            return parent.addField(new ObjectFieldDefinition(fieldName, ValueType.REFERENCE, false, false, targetType));
        }
    }

    public static final class IndexBuilder {
        private final Builder parent;
        private final String indexName;
        private final IndexKind kind;

        private IndexBuilder(Builder parent, String indexName, IndexKind kind) {
            this.parent = parent;
            this.indexName = indexName;
            this.kind = kind;
        }

        public Builder on(String... fieldNames) {
            return parent.addIndex(new IndexDefinition(indexName, kind, List.of(fieldNames)));
        }
    }

    private static List<PropertyDescriptor> beanProperties(Class<?> beanType) {
        try {
            BeanInfo beanInfo = Introspector.getBeanInfo(beanType, Object.class);
            List<PropertyDescriptor> properties = new ArrayList<>();
            for (PropertyDescriptor descriptor : beanInfo.getPropertyDescriptors()) {
                if (descriptor.getReadMethod() != null) {
                    properties.add(descriptor);
                }
            }
            return properties;
        } catch (IntrospectionException e) {
            throw new IllegalArgumentException("Failed to inspect bean type '" + beanType.getSimpleName() + "'", e);
        }
    }

    private static ObjectFieldDefinition inferField(PropertyDescriptor descriptor) {
        Class<?> propertyType = descriptor.getPropertyType();
        String name = descriptor.getName();

        if (propertyType == String.class) {
            return ObjectFieldDefinition.optional(name, ValueType.STRING);
        }
        if (propertyType == long.class) {
            return ObjectFieldDefinition.required(name, ValueType.LONG);
        }
        if (propertyType == int.class || propertyType == short.class || propertyType == byte.class) {
            return ObjectFieldDefinition.required(name, ValueType.LONG);
        }
        if (propertyType == Long.class || propertyType == Integer.class
                || propertyType == Short.class || propertyType == Byte.class) {
            return ObjectFieldDefinition.optional(name, ValueType.LONG);
        }
        if (propertyType == double.class || propertyType == float.class) {
            return ObjectFieldDefinition.required(name, ValueType.DOUBLE);
        }
        if (propertyType == Double.class || propertyType == Float.class) {
            return ObjectFieldDefinition.optional(name, ValueType.DOUBLE);
        }
        if (propertyType == boolean.class) {
            return ObjectFieldDefinition.required(name, ValueType.BOOLEAN);
        }
        if (propertyType == Boolean.class) {
            return ObjectFieldDefinition.optional(name, ValueType.BOOLEAN);
        }
        return null;
    }
}
