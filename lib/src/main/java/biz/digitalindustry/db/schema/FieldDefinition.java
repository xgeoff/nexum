package biz.digitalindustry.db.schema;

public record FieldDefinition(
        String name,
        ValueType type,
        boolean required,
        boolean repeated,
        int vectorDimension
) {
    public FieldDefinition {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("Field name must not be blank");
        }
        if (type == null) {
            throw new IllegalArgumentException("Field type must not be null");
        }
        if (type == ValueType.VECTOR) {
            if (repeated) {
                throw new IllegalArgumentException("Vector field '" + name + "' cannot be repeated");
            }
            if (vectorDimension <= 0) {
                throw new IllegalArgumentException("Vector field '" + name + "' must declare a positive vectorDimension");
            }
        } else if (vectorDimension != 0) {
            throw new IllegalArgumentException("Field '" + name + "' may only declare vectorDimension when type is vector");
        }
    }

    public FieldDefinition(String name, ValueType type, boolean required, boolean repeated) {
        this(name, type, required, repeated, 0);
    }

    public static FieldDefinition vector(String name, int vectorDimension, boolean required) {
        return new FieldDefinition(name, ValueType.VECTOR, required, false, vectorDimension);
    }
}
