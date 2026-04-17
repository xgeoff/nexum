package biz.digitalindustry.db.object.api;

import biz.digitalindustry.db.schema.IndexDefinition;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public record ObjectTypeDefinition(
        String name,
        String keyField,
        List<ObjectFieldDefinition> fields,
        List<IndexDefinition> indexes
) {
    public ObjectTypeDefinition(String name, String keyField, List<ObjectFieldDefinition> fields) {
        this(name, keyField, fields, List.of());
    }

    public ObjectTypeDefinition {
        fields = List.copyOf(fields);
        indexes = List.copyOf(indexes);
    }

    public static ObjectTypeDefinition of(
            String name,
            String keyField,
            List<IndexDefinition> indexes,
            ObjectFieldDefinition... fields
    ) {
        return new ObjectTypeDefinition(name, keyField, List.of(fields), indexes);
    }

    public static ObjectTypeDefinition of(String name, String keyField, ObjectFieldDefinition... fields) {
        return new ObjectTypeDefinition(name, keyField, List.of(fields), List.of());
    }

    public ObjectFieldDefinition field(String fieldName) {
        for (ObjectFieldDefinition field : fields) {
            if (field.name().equals(fieldName)) {
                return field;
            }
        }
        return null;
    }

    public Map<String, ObjectFieldDefinition> fieldsByName() {
        Map<String, ObjectFieldDefinition> fieldMap = new LinkedHashMap<>();
        for (ObjectFieldDefinition field : fields) {
            fieldMap.put(field.name(), field);
        }
        return fieldMap;
    }
}
