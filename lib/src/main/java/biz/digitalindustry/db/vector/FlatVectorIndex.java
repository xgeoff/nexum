package biz.digitalindustry.db.vector;

import biz.digitalindustry.db.index.VectorIndex;
import biz.digitalindustry.db.index.VectorMatch;
import biz.digitalindustry.db.model.Vector;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class FlatVectorIndex<I> implements VectorIndex<I> {
    private final int dimension;
    private final DistanceFunction distanceFunction;
    private final Map<I, Vector> vectors = new LinkedHashMap<>();

    public FlatVectorIndex(int dimension, DistanceFunction distanceFunction) {
        if (dimension <= 0) {
            throw new IllegalArgumentException("dimension must be positive");
        }
        if (distanceFunction == null) {
            throw new IllegalArgumentException("distanceFunction must not be null");
        }
        this.dimension = dimension;
        this.distanceFunction = distanceFunction;
    }

    @Override
    public void add(I identifier, Vector vector) {
        requireDimension(vector);
        vectors.put(identifier, vector);
    }

    @Override
    public void remove(I identifier) {
        vectors.remove(identifier);
    }

    @Override
    public List<VectorMatch<I>> nearest(Vector query, int limit) {
        requireDimension(query);
        if (limit <= 0 || vectors.isEmpty()) {
            return List.of();
        }
        List<VectorMatch<I>> matches = new ArrayList<>(vectors.size());
        for (Map.Entry<I, Vector> entry : vectors.entrySet()) {
            matches.add(new VectorMatch<>(entry.getKey(), distanceFunction.distance(query, entry.getValue())));
        }
        matches.sort(Comparator.comparing(VectorMatch::distance));
        return List.copyOf(matches.subList(0, Math.min(limit, matches.size())));
    }

    public Map<I, Vector> snapshot() {
        return Map.copyOf(vectors);
    }

    public void restore(Map<I, Vector> snapshot) {
        vectors.clear();
        for (Map.Entry<I, Vector> entry : snapshot.entrySet()) {
            add(entry.getKey(), entry.getValue());
        }
    }

    private void requireDimension(Vector vector) {
        if (vector.dimension() != dimension) {
            throw new IllegalArgumentException(
                    "Vector dimension mismatch: expected " + dimension + " but was " + vector.dimension());
        }
    }
}
