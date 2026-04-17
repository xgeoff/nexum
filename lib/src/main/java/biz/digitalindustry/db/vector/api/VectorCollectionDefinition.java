package biz.digitalindustry.db.vector.api;

import biz.digitalindustry.db.schema.EntityType;
import biz.digitalindustry.db.schema.FieldDefinition;
import biz.digitalindustry.db.schema.IndexDefinition;
import biz.digitalindustry.db.schema.IndexKind;
import biz.digitalindustry.db.schema.SchemaDefinition;
import biz.digitalindustry.db.schema.SchemaKind;
import biz.digitalindustry.db.schema.ValueType;
import biz.digitalindustry.db.vector.Distances;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;

public final class VectorCollectionDefinition {
    public static final String INTERNAL_KEY_FIELD = "vectorKey";
    public static final String KEY_COLUMN_INDEX_PREFIX = "__vector_key__";

    private final String name;
    private final String keyField;
    private final String vectorField;
    private final int dimension;
    private final String distanceMetric;
    private final List<FieldDefinition> fields;
    private final List<IndexDefinition> indexes;
    private final EntityType entityType;
    private final SchemaDefinition schemaDefinition;

    public VectorCollectionDefinition(
            String name,
            String keyField,
            String vectorField,
            int dimension,
            String distanceMetric,
            List<FieldDefinition> fields,
            List<IndexDefinition> indexes
    ) {
        this.name = name;
        this.keyField = keyField;
        this.vectorField = vectorField;
        this.dimension = dimension;
        this.distanceMetric = Distances.normalize(distanceMetric);
        this.fields = List.copyOf(fields);
        this.indexes = List.copyOf(indexes);
        validateFields();
        this.entityType = new EntityType(name, mergeFields(), mergeIndexes());
        this.schemaDefinition = SchemaDefinition.of(entityType, SchemaKind.VECTOR);
    }

    public String name() {
        return name;
    }

    public String keyField() {
        return keyField;
    }

    public String vectorField() {
        return vectorField;
    }

    public int dimension() {
        return dimension;
    }

    public String distanceMetric() {
        return distanceMetric;
    }

    public List<FieldDefinition> fields() {
        return fields;
    }

    public List<IndexDefinition> indexes() {
        return indexes;
    }

    public EntityType entityType() {
        return entityType;
    }

    public SchemaDefinition schemaDefinition() {
        return schemaDefinition;
    }

    public static boolean isVectorSchemaDefinition(SchemaDefinition schemaDefinition) {
        if (schemaDefinition.kind() != SchemaKind.VECTOR) {
            return false;
        }
        boolean hasInternalKey = false;
        boolean hasPrimaryIndex = false;
        boolean hasVectorIndex = false;
        for (FieldDefinition field : schemaDefinition.fields()) {
            if (INTERNAL_KEY_FIELD.equals(field.name())) {
                hasInternalKey = true;
                break;
            }
        }
        for (IndexDefinition index : schemaDefinition.indexes()) {
            if (index.kind() == IndexKind.PRIMARY
                    && index.fields().size() == 1
                    && INTERNAL_KEY_FIELD.equals(index.fields().get(0))) {
                hasPrimaryIndex = true;
            }
            if (index.kind() == IndexKind.VECTOR) {
                hasVectorIndex = true;
            }
        }
        return hasInternalKey && hasPrimaryIndex && hasVectorIndex;
    }

    public static VectorCollectionDefinition fromSchemaDefinition(SchemaDefinition schemaDefinition) {
        if (!isVectorSchemaDefinition(schemaDefinition)) {
            throw new IllegalArgumentException("Schema '" + schemaDefinition.name() + "' is not a vector collection definition");
        }
        List<FieldDefinition> fields = new ArrayList<>();
        String keyField = null;
        FieldDefinition vectorField = null;
        for (FieldDefinition field : schemaDefinition.fields()) {
            if (INTERNAL_KEY_FIELD.equals(field.name())) {
                continue;
            }
            fields.add(field);
        }
        List<IndexDefinition> indexes = new ArrayList<>();
        String distanceMetric = null;
        int dimension = 0;
        for (IndexDefinition index : schemaDefinition.indexes()) {
            if (index.name().startsWith(KEY_COLUMN_INDEX_PREFIX) && index.fields().size() == 1) {
                keyField = index.fields().get(0);
                continue;
            }
            if (index.kind() == IndexKind.PRIMARY
                    && index.fields().size() == 1
                    && INTERNAL_KEY_FIELD.equals(index.fields().get(0))) {
                continue;
            }
            if (index.kind() == IndexKind.VECTOR) {
                distanceMetric = index.distanceMetric();
                dimension = index.vectorDimension();
                for (FieldDefinition field : fields) {
                    if (field.name().equals(index.fields().get(0))) {
                        vectorField = field;
                        break;
                    }
                }
            }
            indexes.add(index);
        }
        if (keyField == null || vectorField == null) {
            throw new IllegalArgumentException("Unable to restore vector collection '" + schemaDefinition.name() + "'");
        }
        String resolvedVectorFieldName = vectorField.name();
        indexes.removeIf(index -> index.kind() == IndexKind.VECTOR && index.fields().contains(resolvedVectorFieldName));
        return new VectorCollectionDefinition(
                schemaDefinition.name(),
                keyField,
                vectorField.name(),
                dimension,
                distanceMetric,
                fields,
                indexes
        );
    }

    private void validateFields() {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("Collection name must not be blank");
        }
        if (keyField == null || keyField.isBlank()) {
            throw new IllegalArgumentException("Collection keyField must not be blank");
        }
        if (vectorField == null || vectorField.isBlank()) {
            throw new IllegalArgumentException("Collection vectorField must not be blank");
        }
        if (dimension <= 0) {
            throw new IllegalArgumentException("Collection dimension must be positive");
        }
        boolean foundKey = false;
        boolean foundVector = false;
        for (FieldDefinition field : fields) {
            if (INTERNAL_KEY_FIELD.equals(field.name())) {
                throw new IllegalArgumentException("Field name '" + INTERNAL_KEY_FIELD + "' is reserved for vector identity");
            }
            if (field.name().equals(keyField)) {
                foundKey = true;
            }
            if (field.name().equals(vectorField)) {
                foundVector = true;
                if (field.type() != ValueType.VECTOR) {
                    throw new IllegalArgumentException("Vector field '" + vectorField + "' must use ValueType.VECTOR");
                }
                if (field.vectorDimension() != dimension) {
                    throw new IllegalArgumentException("Vector field '" + vectorField + "' must declare dimension " + dimension);
                }
            }
        }
        if (!foundKey) {
            throw new IllegalArgumentException("Key field '" + keyField + "' is not present in collection '" + name + "'");
        }
        if (!foundVector) {
            throw new IllegalArgumentException("Vector field '" + vectorField + "' is not present in collection '" + name + "'");
        }
    }

    private List<FieldDefinition> mergeFields() {
        List<FieldDefinition> merged = new ArrayList<>();
        merged.add(new FieldDefinition(INTERNAL_KEY_FIELD, ValueType.STRING, true, false));
        merged.addAll(fields);
        return List.copyOf(merged);
    }

    private List<IndexDefinition> mergeIndexes() {
        List<IndexDefinition> merged = new ArrayList<>();
        merged.add(new IndexDefinition("vector_key_pk", IndexKind.PRIMARY, List.of(INTERNAL_KEY_FIELD)));
        merged.add(new IndexDefinition(KEY_COLUMN_INDEX_PREFIX + keyField, IndexKind.UNIQUE, List.of(keyField)));
        merged.add(IndexDefinition.vector(vectorField + "_vector_idx", vectorField, dimension, distanceMetric));
        LinkedHashSet<IndexDefinition> deduped = new LinkedHashSet<>(indexes);
        merged.addAll(deduped);
        return List.copyOf(merged);
    }
}
