package biz.digitalindustry.db.object.api;

import biz.digitalindustry.db.schema.EntityType;
import biz.digitalindustry.db.schema.FieldDefinition;
import biz.digitalindustry.db.schema.IndexDefinition;
import biz.digitalindustry.db.schema.IndexKind;
import biz.digitalindustry.db.schema.SchemaDefinition;
import biz.digitalindustry.db.schema.SchemaKind;
import biz.digitalindustry.db.schema.ValueType;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;

public final class ObjectType<T> {
    public static final String KEY_FIELD = "objectKey";

    private final String name;
    private final List<FieldDefinition> fields;
    private final List<IndexDefinition> indexes;
    private final ObjectCodec<T> codec;
    private final EntityType entityType;
    private final SchemaDefinition schemaDefinition;

    public ObjectType(String name, List<FieldDefinition> fields, List<IndexDefinition> indexes, ObjectCodec<T> codec) {
        this.name = name;
        this.fields = List.copyOf(fields);
        this.indexes = List.copyOf(indexes);
        this.codec = codec;
        validateFields(this.fields);
        this.entityType = new EntityType(name, mergeFields(this.fields), mergeIndexes(this.indexes));
        this.schemaDefinition = SchemaDefinition.of(entityType, SchemaKind.OBJECT);
    }

    public String name() {
        return name;
    }

    public List<FieldDefinition> fields() {
        return fields;
    }

    public List<IndexDefinition> indexes() {
        return indexes;
    }

    public ObjectCodec<T> codec() {
        return codec;
    }

    public EntityType entityType() {
        return entityType;
    }

    public SchemaDefinition schemaDefinition() {
        return schemaDefinition;
    }

    public static boolean isObjectEntityType(EntityType entityType) {
        boolean hasKeyField = false;
        boolean hasPrimaryIndex = false;
        for (FieldDefinition field : entityType.fields()) {
            if (KEY_FIELD.equals(field.name())) {
                hasKeyField = true;
                break;
            }
        }
        for (IndexDefinition index : entityType.indexes()) {
            if (index.kind() == IndexKind.PRIMARY
                    && index.fields().size() == 1
                    && KEY_FIELD.equals(index.fields().get(0))) {
                hasPrimaryIndex = true;
                break;
            }
        }
        return hasKeyField && hasPrimaryIndex;
    }

    private static void validateFields(List<FieldDefinition> fields) {
        for (FieldDefinition field : fields) {
            if (KEY_FIELD.equals(field.name())) {
                throw new IllegalArgumentException("Field name '" + KEY_FIELD + "' is reserved for object identity");
            }
        }
    }

    private static List<FieldDefinition> mergeFields(List<FieldDefinition> fields) {
        List<FieldDefinition> merged = new ArrayList<>();
        merged.add(new FieldDefinition(KEY_FIELD, ValueType.STRING, true, false));
        merged.addAll(fields);
        return List.copyOf(merged);
    }

    private static List<IndexDefinition> mergeIndexes(List<IndexDefinition> indexes) {
        List<IndexDefinition> merged = new ArrayList<>();
        merged.add(new IndexDefinition("object_key_pk", IndexKind.PRIMARY, List.of(KEY_FIELD)));
        LinkedHashSet<IndexDefinition> deduped = new LinkedHashSet<>(indexes);
        merged.addAll(deduped);
        return List.copyOf(merged);
    }
}
