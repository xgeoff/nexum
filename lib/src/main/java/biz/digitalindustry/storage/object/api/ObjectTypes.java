package biz.digitalindustry.storage.object.api;

import biz.digitalindustry.storage.schema.IndexDefinition;
import biz.digitalindustry.storage.schema.IndexKind;
import biz.digitalindustry.storage.schema.ValueType;

import java.util.ArrayList;
import java.util.List;

public final class ObjectTypes {
    private ObjectTypes() {
    }

    public static Builder define(String name) {
        return new Builder(name);
    }

    public static final class Builder {
        private final String name;
        private final List<ObjectFieldDefinition> fields = new ArrayList<>();
        private final List<IndexDefinition> indexes = new ArrayList<>();
        private String keyField;

        private Builder(String name) {
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
            return new ObjectTypeDefinition(name, keyField, fields, indexes);
        }

        private Builder addField(ObjectFieldDefinition field) {
            fields.add(field);
            return this;
        }

        private Builder addIndex(IndexDefinition index) {
            indexes.add(index);
            return this;
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
}
