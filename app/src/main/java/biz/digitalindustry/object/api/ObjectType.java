package biz.digitalindustry.object.api;

import biz.digitalindustry.storage.schema.EntityType;
import biz.digitalindustry.storage.schema.FieldDefinition;
import biz.digitalindustry.storage.schema.IndexDefinition;
import biz.digitalindustry.storage.schema.IndexKind;
import biz.digitalindustry.storage.schema.ValueType;

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

    public ObjectType(String name, List<FieldDefinition> fields, List<IndexDefinition> indexes, ObjectCodec<T> codec) {
        this.name = name;
        this.fields = List.copyOf(fields);
        this.indexes = List.copyOf(indexes);
        this.codec = codec;
        validateFields(this.fields);
        this.entityType = new EntityType(name, mergeFields(this.fields), mergeIndexes(this.indexes));
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
