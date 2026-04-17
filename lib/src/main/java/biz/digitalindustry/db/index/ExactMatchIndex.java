package biz.digitalindustry.db.index;

import biz.digitalindustry.db.model.FieldValue;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

public final class ExactMatchIndex<I> {
    private final Map<String, Map<FieldValue, Set<I>>> valuesByField = new LinkedHashMap<>();

    public ExactMatchIndex(Collection<String> indexedFields) {
        for (String field : indexedFields) {
            valuesByField.put(field, new LinkedHashMap<>());
        }
    }

    public boolean hasField(String fieldName) {
        return valuesByField.containsKey(fieldName);
    }

    public Set<I> find(String fieldName, FieldValue value) {
        Map<FieldValue, Set<I>> fieldIndex = valuesByField.get(fieldName);
        if (fieldIndex == null) {
            return Set.of();
        }
        return new LinkedHashSet<>(fieldIndex.getOrDefault(value, Set.of()));
    }

    public Set<String> fields() {
        return new LinkedHashSet<>(valuesByField.keySet());
    }

    public void add(Map<String, FieldValue> fieldValues, I identifier) {
        for (String fieldName : valuesByField.keySet()) {
            FieldValue value = fieldValues.get(fieldName);
            if (value == null) {
                continue;
            }
            valuesByField.get(fieldName)
                    .computeIfAbsent(value, ignored -> new LinkedHashSet<>())
                    .add(identifier);
        }
    }

    public void remove(Map<String, FieldValue> fieldValues, I identifier) {
        for (String fieldName : valuesByField.keySet()) {
            FieldValue value = fieldValues.get(fieldName);
            if (value == null) {
                continue;
            }
            Map<FieldValue, Set<I>> fieldIndex = valuesByField.get(fieldName);
            Set<I> identifiers = fieldIndex.get(value);
            if (identifiers == null) {
                continue;
            }
            identifiers.remove(identifier);
            if (identifiers.isEmpty()) {
                fieldIndex.remove(value);
            }
        }
    }

    public Map<FieldValue, Set<I>> snapshot(String fieldName) {
        Map<FieldValue, Set<I>> fieldIndex = valuesByField.get(fieldName);
        Map<FieldValue, Set<I>> snapshot = new LinkedHashMap<>();
        if (fieldIndex == null) {
            return snapshot;
        }
        for (Map.Entry<FieldValue, Set<I>> entry : fieldIndex.entrySet()) {
            snapshot.put(entry.getKey(), new LinkedHashSet<>(entry.getValue()));
        }
        return snapshot;
    }

    public void restore(Map<String, Map<FieldValue, Set<I>>> snapshot) {
        valuesByField.clear();
        for (Map.Entry<String, Map<FieldValue, Set<I>>> fieldEntry : snapshot.entrySet()) {
            Map<FieldValue, Set<I>> restoredValues = new LinkedHashMap<>();
            for (Map.Entry<FieldValue, Set<I>> valueEntry : fieldEntry.getValue().entrySet()) {
                restoredValues.put(valueEntry.getKey(), new LinkedHashSet<>(valueEntry.getValue()));
            }
            valuesByField.put(fieldEntry.getKey(), restoredValues);
        }
    }
}
