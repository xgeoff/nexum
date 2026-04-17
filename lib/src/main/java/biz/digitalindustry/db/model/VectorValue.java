package biz.digitalindustry.db.model;

public record VectorValue(Vector vector) implements FieldValue {
    public VectorValue {
        if (vector == null) {
            throw new IllegalArgumentException("vector must not be null");
        }
    }
}
