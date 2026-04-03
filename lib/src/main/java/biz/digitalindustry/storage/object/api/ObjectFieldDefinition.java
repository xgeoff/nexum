package biz.digitalindustry.storage.object.api;

import biz.digitalindustry.storage.schema.ValueType;

public record ObjectFieldDefinition(
        String name,
        ValueType type,
        boolean required,
        boolean repeated,
        String referenceTarget
) {
    public static ObjectFieldDefinition required(String name, ValueType type) {
        return new ObjectFieldDefinition(name, type, true, false, null);
    }

    public static ObjectFieldDefinition optional(String name, ValueType type) {
        return new ObjectFieldDefinition(name, type, false, false, null);
    }

    public static ObjectFieldDefinition requiredReference(String name, String referenceTarget) {
        return new ObjectFieldDefinition(name, ValueType.REFERENCE, true, false, referenceTarget);
    }

    public static ObjectFieldDefinition optionalReference(String name, String referenceTarget) {
        return new ObjectFieldDefinition(name, ValueType.REFERENCE, false, false, referenceTarget);
    }
}
