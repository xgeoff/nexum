package biz.digitalindustry.db.engine;

import biz.digitalindustry.db.index.VectorMatch;
import biz.digitalindustry.db.model.Vector;
import biz.digitalindustry.db.vector.Distances;
import biz.digitalindustry.db.vector.FlatVectorIndex;
import biz.digitalindustry.db.vector.VectorIndexDefinition;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class VectorIndexManager {
    private final Map<String, Map<String, ManagedVectorIndex<Object>>> indexesByNamespace = new LinkedHashMap<>();

    public synchronized boolean ensureNamespace(String namespace, Collection<VectorIndexDefinition> definitions) {
        Map<String, ManagedVectorIndex<Object>> existing = indexesByNamespace.get(namespace);
        Map<String, ManagedVectorIndex<Object>> requested = new LinkedHashMap<>();
        for (VectorIndexDefinition definition : definitions) {
            requested.put(
                    definition.field(),
                    new ManagedVectorIndex<>(
                            definition.dimension(),
                            definition.distanceMetric(),
                            new FlatVectorIndex<>(definition.dimension(), Distances.resolve(definition.distanceMetric()))
                    )
            );
        }
        if (existing != null && definitionsMatch(existing, requested)) {
            return true;
        }
        indexesByNamespace.put(namespace, requested);
        return false;
    }

    public synchronized List<VectorMatch<Object>> nearest(String namespace, String fieldName, Vector query, int limit) {
        ManagedVectorIndex<Object> index = index(namespace, fieldName);
        return index == null ? List.of() : index.index().nearest(query, limit);
    }

    public synchronized void add(String namespace, String fieldName, Vector value, Object identifier) {
        ManagedVectorIndex<Object> index = index(namespace, fieldName);
        if (index != null && value != null) {
            index.index().add(identifier, value);
        }
    }

    public synchronized void remove(String namespace, String fieldName, Object identifier) {
        ManagedVectorIndex<Object> index = index(namespace, fieldName);
        if (index != null) {
            index.index().remove(identifier);
        }
    }

    public synchronized boolean hasField(String namespace, String fieldName) {
        return index(namespace, fieldName) != null;
    }

    public synchronized Set<String> namespaces() {
        return new LinkedHashSet<>(indexesByNamespace.keySet());
    }

    public synchronized void clear() {
        indexesByNamespace.clear();
    }

    public synchronized VectorIndexManager copy() {
        VectorIndexManager copy = new VectorIndexManager();
        for (Map.Entry<String, Map<String, ManagedVectorIndex<Object>>> namespaceEntry : indexesByNamespace.entrySet()) {
            Map<String, ManagedVectorIndex<Object>> namespaceCopy = new LinkedHashMap<>();
            for (Map.Entry<String, ManagedVectorIndex<Object>> fieldEntry : namespaceEntry.getValue().entrySet()) {
                ManagedVectorIndex<Object> index = fieldEntry.getValue();
                FlatVectorIndex<Object> indexCopy = new FlatVectorIndex<>(
                        index.dimension(),
                        Distances.resolve(index.distanceMetric())
                );
                indexCopy.restore(index.index().snapshot());
                namespaceCopy.put(fieldEntry.getKey(), new ManagedVectorIndex<>(index.dimension(), index.distanceMetric(), indexCopy));
            }
            copy.indexesByNamespace.put(namespaceEntry.getKey(), namespaceCopy);
        }
        return copy;
    }

    private boolean definitionsMatch(
            Map<String, ManagedVectorIndex<Object>> existing,
            Map<String, ManagedVectorIndex<Object>> requested
    ) {
        if (!existing.keySet().equals(requested.keySet())) {
            return false;
        }
        for (Map.Entry<String, ManagedVectorIndex<Object>> entry : existing.entrySet()) {
            ManagedVectorIndex<Object> candidate = requested.get(entry.getKey());
            if (candidate == null) {
                return false;
            }
            ManagedVectorIndex<Object> current = entry.getValue();
            if (current.dimension() != candidate.dimension()
                    || !current.distanceMetric().equals(candidate.distanceMetric())) {
                return false;
            }
        }
        return true;
    }

    private ManagedVectorIndex<Object> index(String namespace, String fieldName) {
        Map<String, ManagedVectorIndex<Object>> namespaceIndexes = indexesByNamespace.get(namespace);
        return namespaceIndexes == null ? null : namespaceIndexes.get(fieldName);
    }

    private record ManagedVectorIndex<I>(
            int dimension,
            String distanceMetric,
            FlatVectorIndex<I> index
    ) {
    }
}
