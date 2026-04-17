package biz.digitalindustry.db.schema;

import java.util.List;

public record IndexDefinition(
        String name,
        IndexKind kind,
        List<String> fields,
        int vectorDimension,
        String distanceMetric
) {
    public IndexDefinition {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("Index name must not be blank");
        }
        if (kind == null) {
            throw new IllegalArgumentException("Index kind must not be null");
        }
        fields = List.copyOf(fields);
        if (kind == IndexKind.VECTOR) {
            if (fields.size() != 1) {
                throw new IllegalArgumentException("Vector index '" + name + "' must target exactly one field");
            }
            if (vectorDimension <= 0) {
                throw new IllegalArgumentException("Vector index '" + name + "' must declare a positive vectorDimension");
            }
            if (distanceMetric == null || distanceMetric.isBlank()) {
                throw new IllegalArgumentException("Vector index '" + name + "' must declare a distanceMetric");
            }
        } else {
            if (vectorDimension != 0) {
                throw new IllegalArgumentException("Index '" + name + "' may only declare vectorDimension when kind is vector");
            }
            if (distanceMetric != null) {
                throw new IllegalArgumentException("Index '" + name + "' may only declare distanceMetric when kind is vector");
            }
        }
    }

    public IndexDefinition(String name, IndexKind kind, List<String> fields) {
        this(name, kind, fields, 0, null);
    }

    public static IndexDefinition vector(String name, String field, int vectorDimension, String distanceMetric) {
        return new IndexDefinition(name, IndexKind.VECTOR, List.of(field), vectorDimension, distanceMetric);
    }
}
