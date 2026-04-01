package biz.digitalindustry.storage.schema;

public record FieldDefinition(
        String name,
        ValueType type,
        boolean required,
        boolean repeated
) {
}
