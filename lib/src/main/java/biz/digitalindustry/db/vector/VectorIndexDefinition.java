package biz.digitalindustry.db.vector;

public record VectorIndexDefinition(
        String field,
        int dimension,
        String distanceMetric
) {
    public VectorIndexDefinition {
        if (field == null || field.isBlank()) {
            throw new IllegalArgumentException("field must not be blank");
        }
        if (dimension <= 0) {
            throw new IllegalArgumentException("dimension must be positive");
        }
        distanceMetric = Distances.normalize(distanceMetric);
    }
}
