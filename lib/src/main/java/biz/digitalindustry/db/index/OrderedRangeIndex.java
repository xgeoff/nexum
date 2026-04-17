package biz.digitalindustry.db.index;

import biz.digitalindustry.db.model.BooleanValue;
import biz.digitalindustry.db.model.DoubleValue;
import biz.digitalindustry.db.model.FieldValue;
import biz.digitalindustry.db.model.LongValue;
import biz.digitalindustry.db.model.StringValue;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

public final class OrderedRangeIndex<I> {
    private final NavigableMap<ComparableFieldValue, Set<I>> values = new TreeMap<>();

    public OrderedRangeIndex() {
    }

    public void add(FieldValue value, I identifier) {
        if (value == null) {
            return;
        }
        values.computeIfAbsent(comparable(value), ignored -> new LinkedHashSet<>()).add(identifier);
    }

    public void remove(FieldValue value, I identifier) {
        if (value == null) {
            return;
        }
        Set<I> identifiers = values.get(comparable(value));
        if (identifiers == null) {
            return;
        }
        identifiers.remove(identifier);
        if (identifiers.isEmpty()) {
            values.remove(comparable(value));
        }
    }

    public Set<I> range(FieldValue fromInclusive, FieldValue toInclusive) {
        NavigableMap<ComparableFieldValue, Set<I>> window = values.subMap(
                comparable(fromInclusive),
                true,
                comparable(toInclusive),
                true
        );
        Set<I> matches = new LinkedHashSet<>();
        for (Set<I> identifiers : window.values()) {
            matches.addAll(identifiers);
        }
        return matches;
    }

    public OrderedRangeIndex<I> copy() {
        OrderedRangeIndex<I> copy = new OrderedRangeIndex<>();
        for (var entry : values.entrySet()) {
            copy.values.put(entry.getKey(), new LinkedHashSet<>(entry.getValue()));
        }
        return copy;
    }

    public NavigableMap<ComparableFieldValue, Set<I>> snapshot() {
        NavigableMap<ComparableFieldValue, Set<I>> snapshot = new TreeMap<>();
        for (var entry : values.entrySet()) {
            snapshot.put(entry.getKey(), new LinkedHashSet<>(entry.getValue()));
        }
        return snapshot;
    }

    public Map<FieldValue, Set<I>> snapshotValues() {
        Map<FieldValue, Set<I>> snapshot = new LinkedHashMap<>();
        for (var entry : values.entrySet()) {
            snapshot.put(entry.getKey().toFieldValue(), new LinkedHashSet<>(entry.getValue()));
        }
        return snapshot;
    }

    public void restore(NavigableMap<ComparableFieldValue, Set<I>> snapshot) {
        values.clear();
        for (var entry : snapshot.entrySet()) {
            values.put(entry.getKey(), new LinkedHashSet<>(entry.getValue()));
        }
    }

    public void restoreValues(Map<FieldValue, Set<I>> snapshot) {
        values.clear();
        for (var entry : snapshot.entrySet()) {
            values.put(comparable(entry.getKey()), new LinkedHashSet<>(entry.getValue()));
        }
    }

    public static int compare(FieldValue left, FieldValue right) {
        return comparable(left).compareTo(comparable(right));
    }

    private static ComparableFieldValue comparable(FieldValue value) {
        if (value instanceof StringValue stringValue) {
            return new ComparableFieldValue(1, stringValue.value());
        }
        if (value instanceof LongValue longValue) {
            return new ComparableFieldValue(2, longValue.value());
        }
        if (value instanceof DoubleValue doubleValue) {
            return new ComparableFieldValue(3, doubleValue.value());
        }
        if (value instanceof BooleanValue booleanValue) {
            return new ComparableFieldValue(4, booleanValue.value());
        }
        throw new IllegalArgumentException("Unsupported ordered field value: " + value);
    }

    public record ComparableFieldValue(int kindOrder, Comparable<?> value) implements Comparable<ComparableFieldValue> {
        public FieldValue toFieldValue() {
            return switch (kindOrder) {
                case 1 -> new StringValue((String) value);
                case 2 -> new LongValue((Long) value);
                case 3 -> new DoubleValue((Double) value);
                case 4 -> new BooleanValue((Boolean) value);
                default -> throw new IllegalStateException("Unsupported ordered field kind " + kindOrder);
            };
        }

        @Override
        @SuppressWarnings({"rawtypes", "unchecked"})
        public int compareTo(ComparableFieldValue other) {
            int kindCompare = Integer.compare(kindOrder, other.kindOrder);
            if (kindCompare != 0) {
                return kindCompare;
            }
            return ((Comparable) value).compareTo(other.value);
        }
    }
}
