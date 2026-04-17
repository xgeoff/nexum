package biz.digitalindustry.db.vector;

import biz.digitalindustry.db.model.Vector;

public final class Distances {
    public static final String EUCLIDEAN = "euclidean";
    public static final String COSINE = "cosine";

    private Distances() {
    }

    public static DistanceFunction resolve(String name) {
        return switch (normalize(name)) {
            case EUCLIDEAN -> Distances::euclidean;
            case COSINE -> Distances::cosine;
            default -> throw new IllegalArgumentException("Unsupported distance metric '" + name + "'");
        };
    }

    public static String normalize(String name) {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("Distance metric must not be blank");
        }
        return name.trim().toLowerCase(java.util.Locale.ROOT);
    }

    public static float euclidean(Vector left, Vector right) {
        requireSameDimension(left, right);
        float sum = 0.0f;
        for (int i = 0; i < left.dimension(); i++) {
            float delta = left.value(i) - right.value(i);
            sum += delta * delta;
        }
        return (float) Math.sqrt(sum);
    }

    public static float cosine(Vector left, Vector right) {
        requireSameDimension(left, right);
        float dot = 0.0f;
        float leftNorm = 0.0f;
        float rightNorm = 0.0f;
        for (int i = 0; i < left.dimension(); i++) {
            float leftValue = left.value(i);
            float rightValue = right.value(i);
            dot += leftValue * rightValue;
            leftNorm += leftValue * leftValue;
            rightNorm += rightValue * rightValue;
        }
        if (leftNorm == 0.0f || rightNorm == 0.0f) {
            return 1.0f;
        }
        float similarity = (float) (dot / (Math.sqrt(leftNorm) * Math.sqrt(rightNorm)));
        return 1.0f - similarity;
    }

    public static void requireSameDimension(Vector left, Vector right) {
        if (left.dimension() != right.dimension()) {
            throw new IllegalArgumentException(
                    "Vector dimensions do not match: " + left.dimension() + " != " + right.dimension());
        }
    }
}
