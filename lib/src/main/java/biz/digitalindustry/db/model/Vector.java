package biz.digitalindustry.db.model;

import java.util.Arrays;

public final class Vector {
    private final float[] values;

    public Vector(float[] values) {
        if (values == null || values.length == 0) {
            throw new IllegalArgumentException("Vector values must not be null or empty");
        }
        this.values = Arrays.copyOf(values, values.length);
    }

    public static Vector of(float... values) {
        return new Vector(values);
    }

    public int dimension() {
        return values.length;
    }

    public float value(int index) {
        return values[index];
    }

    public float[] values() {
        return Arrays.copyOf(values, values.length);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof Vector vector)) {
            return false;
        }
        return Arrays.equals(values, vector.values);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(values);
    }

    @Override
    public String toString() {
        return "Vector" + Arrays.toString(values);
    }
}
