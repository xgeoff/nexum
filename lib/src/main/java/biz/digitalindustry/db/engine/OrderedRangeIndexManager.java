package biz.digitalindustry.db.engine;

import biz.digitalindustry.db.index.OrderedRangeIndex;
import biz.digitalindustry.db.model.BooleanValue;
import biz.digitalindustry.db.model.DoubleValue;
import biz.digitalindustry.db.model.FieldValue;
import biz.digitalindustry.db.model.LongValue;
import biz.digitalindustry.db.model.RecordId;
import biz.digitalindustry.db.model.ReferenceValue;
import biz.digitalindustry.db.model.StringValue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class OrderedRangeIndexManager {
    private static final int VERSION = 1;
    private static final byte FIELD_STRING = 1;
    private static final byte FIELD_LONG = 2;
    private static final byte FIELD_DOUBLE = 3;
    private static final byte FIELD_BOOLEAN = 4;
    private static final byte FIELD_REFERENCE = 5;

    private static final byte IDENTIFIER_RECORD_ID = 1;
    private static final byte IDENTIFIER_STRING = 2;

    private final Map<String, Map<String, OrderedRangeIndex<Object>>> indexesByNamespace = new LinkedHashMap<>();

    public synchronized boolean ensureNamespace(String namespace, Collection<String> indexedFields) {
        Map<String, OrderedRangeIndex<Object>> existing = indexesByNamespace.get(namespace);
        Set<String> requestedFields = new LinkedHashSet<>(indexedFields);
        if (existing != null && existing.keySet().equals(requestedFields)) {
            return true;
        }
        Map<String, OrderedRangeIndex<Object>> namespaceIndexes = new LinkedHashMap<>();
        for (String field : requestedFields) {
            namespaceIndexes.put(field, new OrderedRangeIndex<>());
        }
        indexesByNamespace.put(namespace, namespaceIndexes);
        return false;
    }

    public synchronized boolean hasField(String namespace, String fieldName) {
        return indexesByNamespace.containsKey(namespace) && indexesByNamespace.get(namespace).containsKey(fieldName);
    }

    public synchronized Set<Object> range(String namespace, String fieldName, FieldValue fromInclusive, FieldValue toInclusive) {
        Map<String, OrderedRangeIndex<Object>> namespaceIndexes = indexesByNamespace.get(namespace);
        if (namespaceIndexes == null) {
            return Set.of();
        }
        OrderedRangeIndex<Object> index = namespaceIndexes.get(fieldName);
        return index == null ? Set.of() : index.range(fromInclusive, toInclusive);
    }

    public synchronized void add(String namespace, String fieldName, FieldValue value, Object identifier) {
        Map<String, OrderedRangeIndex<Object>> namespaceIndexes = indexesByNamespace.get(namespace);
        if (namespaceIndexes == null) {
            return;
        }
        OrderedRangeIndex<Object> index = namespaceIndexes.get(fieldName);
        if (index != null) {
            index.add(value, identifier);
        }
    }

    public synchronized void remove(String namespace, String fieldName, FieldValue value, Object identifier) {
        Map<String, OrderedRangeIndex<Object>> namespaceIndexes = indexesByNamespace.get(namespace);
        if (namespaceIndexes == null) {
            return;
        }
        OrderedRangeIndex<Object> index = namespaceIndexes.get(fieldName);
        if (index != null) {
            index.remove(value, identifier);
        }
    }

    public synchronized OrderedRangeIndexManager copy() {
        OrderedRangeIndexManager copy = new OrderedRangeIndexManager();
        for (var namespaceEntry : indexesByNamespace.entrySet()) {
            Map<String, OrderedRangeIndex<Object>> namespaceCopy = new LinkedHashMap<>();
            for (var fieldEntry : namespaceEntry.getValue().entrySet()) {
                namespaceCopy.put(fieldEntry.getKey(), fieldEntry.getValue().copy());
            }
            copy.indexesByNamespace.put(namespaceEntry.getKey(), namespaceCopy);
        }
        return copy;
    }

    public synchronized Set<String> namespaces() {
        return new LinkedHashSet<>(indexesByNamespace.keySet());
    }

    public synchronized byte[] encode() {
        try {
            ByteArrayOutputStream buffer = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(buffer);
            out.writeInt(VERSION);
            out.writeInt(indexesByNamespace.size());
            for (Map.Entry<String, Map<String, OrderedRangeIndex<Object>>> namespaceEntry : indexesByNamespace.entrySet()) {
                out.writeUTF(namespaceEntry.getKey());
                out.writeInt(namespaceEntry.getValue().size());
                for (Map.Entry<String, OrderedRangeIndex<Object>> fieldEntry : namespaceEntry.getValue().entrySet()) {
                    out.writeUTF(fieldEntry.getKey());
                    Map<FieldValue, Set<Object>> snapshot = fieldEntry.getValue().snapshotValues();
                    out.writeInt(snapshot.size());
                    for (Map.Entry<FieldValue, Set<Object>> valueEntry : snapshot.entrySet()) {
                        writeFieldValue(out, valueEntry.getKey());
                        out.writeInt(valueEntry.getValue().size());
                        for (Object identifier : valueEntry.getValue()) {
                            writeIdentifier(out, identifier);
                        }
                    }
                }
            }
            out.flush();
            return buffer.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Failed to encode ordered range index manager state", e);
        }
    }

    public synchronized byte[] encodeNamespace(String namespace) {
        Map<String, OrderedRangeIndex<Object>> namespaceIndexes = indexesByNamespace.get(namespace);
        if (namespaceIndexes == null) {
            return new byte[0];
        }
        try {
            ByteArrayOutputStream buffer = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(buffer);
            out.writeInt(VERSION);
            out.writeInt(namespaceIndexes.size());
            for (Map.Entry<String, OrderedRangeIndex<Object>> fieldEntry : namespaceIndexes.entrySet()) {
                out.writeUTF(fieldEntry.getKey());
                Map<FieldValue, Set<Object>> snapshot = fieldEntry.getValue().snapshotValues();
                out.writeInt(snapshot.size());
                for (Map.Entry<FieldValue, Set<Object>> valueEntry : snapshot.entrySet()) {
                    writeFieldValue(out, valueEntry.getKey());
                    out.writeInt(valueEntry.getValue().size());
                    for (Object identifier : valueEntry.getValue()) {
                        writeIdentifier(out, identifier);
                    }
                }
            }
            out.flush();
            return buffer.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Failed to encode ordered range index namespace " + namespace, e);
        }
    }

    public synchronized Map<String, Map<FieldValue, Set<Object>>> snapshotNamespace(String namespace) {
        Map<String, OrderedRangeIndex<Object>> namespaceIndexes = indexesByNamespace.get(namespace);
        Map<String, Map<FieldValue, Set<Object>>> snapshot = new LinkedHashMap<>();
        if (namespaceIndexes == null) {
            return snapshot;
        }
        for (Map.Entry<String, OrderedRangeIndex<Object>> fieldEntry : namespaceIndexes.entrySet()) {
            snapshot.put(fieldEntry.getKey(), fieldEntry.getValue().snapshotValues());
        }
        return snapshot;
    }

    public synchronized void restoreNamespace(String namespace, Map<String, Map<FieldValue, Set<Object>>> fieldSnapshots) {
        Map<String, OrderedRangeIndex<Object>> namespaceIndexes = new LinkedHashMap<>();
        for (Map.Entry<String, Map<FieldValue, Set<Object>>> fieldEntry : fieldSnapshots.entrySet()) {
            OrderedRangeIndex<Object> index = new OrderedRangeIndex<>();
            index.restoreValues(fieldEntry.getValue());
            namespaceIndexes.put(fieldEntry.getKey(), index);
        }
        indexesByNamespace.put(namespace, namespaceIndexes);
    }

    public synchronized void decodeNamespace(String namespace, byte[] payload) {
        if (payload == null || payload.length == 0) {
            indexesByNamespace.remove(namespace);
            return;
        }
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(payload));
            int version = in.readInt();
            if (version != VERSION) {
                throw new IOException("Unsupported ordered range index namespace version " + version);
            }
            int fieldCount = in.readInt();
            Map<String, OrderedRangeIndex<Object>> namespaceIndexes = new LinkedHashMap<>();
            for (int fieldIndex = 0; fieldIndex < fieldCount; fieldIndex++) {
                String field = in.readUTF();
                int valueCount = in.readInt();
                Map<FieldValue, Set<Object>> values = new LinkedHashMap<>();
                for (int valueIndex = 0; valueIndex < valueCount; valueIndex++) {
                    FieldValue fieldValue = readFieldValue(in);
                    int identifierCount = in.readInt();
                    Set<Object> identifiers = new LinkedHashSet<>();
                    for (int idIndex = 0; idIndex < identifierCount; idIndex++) {
                        identifiers.add(readIdentifier(in));
                    }
                    values.put(fieldValue, identifiers);
                }
                OrderedRangeIndex<Object> index = new OrderedRangeIndex<>();
                index.restoreValues(values);
                namespaceIndexes.put(field, index);
            }
            indexesByNamespace.put(namespace, namespaceIndexes);
        } catch (IOException e) {
            throw new RuntimeException("Failed to decode ordered range index namespace " + namespace, e);
        }
    }

    public synchronized void decode(byte[] payload) {
        indexesByNamespace.clear();
        if (payload == null || payload.length == 0) {
            return;
        }
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(payload));
            int version = in.readInt();
            if (version != VERSION) {
                throw new IOException("Unsupported ordered range index manager version " + version);
            }
            int namespaceCount = in.readInt();
            for (int i = 0; i < namespaceCount; i++) {
                String namespace = in.readUTF();
                int fieldCount = in.readInt();
                Map<String, OrderedRangeIndex<Object>> namespaceIndexes = new LinkedHashMap<>();
                for (int fieldIndex = 0; fieldIndex < fieldCount; fieldIndex++) {
                    String field = in.readUTF();
                    int valueCount = in.readInt();
                    Map<FieldValue, Set<Object>> values = new LinkedHashMap<>();
                    for (int valueIndex = 0; valueIndex < valueCount; valueIndex++) {
                        FieldValue fieldValue = readFieldValue(in);
                        int identifierCount = in.readInt();
                        Set<Object> identifiers = new LinkedHashSet<>();
                        for (int idIndex = 0; idIndex < identifierCount; idIndex++) {
                            identifiers.add(readIdentifier(in));
                        }
                        values.put(fieldValue, identifiers);
                    }
                    OrderedRangeIndex<Object> index = new OrderedRangeIndex<>();
                    index.restoreValues(values);
                    namespaceIndexes.put(field, index);
                }
                indexesByNamespace.put(namespace, namespaceIndexes);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to decode ordered range index manager state", e);
        }
    }

    public synchronized void clear() {
        indexesByNamespace.clear();
    }

    private void writeFieldValue(DataOutputStream out, FieldValue value) throws IOException {
        if (value instanceof StringValue stringValue) {
            out.writeByte(FIELD_STRING);
            out.writeUTF(stringValue.value());
            return;
        }
        if (value instanceof LongValue longValue) {
            out.writeByte(FIELD_LONG);
            out.writeLong(longValue.value());
            return;
        }
        if (value instanceof DoubleValue doubleValue) {
            out.writeByte(FIELD_DOUBLE);
            out.writeDouble(doubleValue.value());
            return;
        }
        if (value instanceof BooleanValue booleanValue) {
            out.writeByte(FIELD_BOOLEAN);
            out.writeBoolean(booleanValue.value());
            return;
        }
        if (value instanceof ReferenceValue referenceValue) {
            out.writeByte(FIELD_REFERENCE);
            out.writeLong(referenceValue.recordId().value());
            return;
        }
        throw new IOException("Unsupported ordered indexed field value: " + value);
    }

    private FieldValue readFieldValue(DataInputStream in) throws IOException {
        byte type = in.readByte();
        return switch (type) {
            case FIELD_STRING -> new StringValue(in.readUTF());
            case FIELD_LONG -> new LongValue(in.readLong());
            case FIELD_DOUBLE -> new DoubleValue(in.readDouble());
            case FIELD_BOOLEAN -> new BooleanValue(in.readBoolean());
            case FIELD_REFERENCE -> new ReferenceValue(new RecordId(in.readLong()));
            default -> throw new IOException("Unsupported ordered indexed field value type " + type);
        };
    }

    private void writeIdentifier(DataOutputStream out, Object identifier) throws IOException {
        if (identifier instanceof RecordId recordId) {
            out.writeByte(IDENTIFIER_RECORD_ID);
            out.writeLong(recordId.value());
            return;
        }
        if (identifier instanceof String stringValue) {
            out.writeByte(IDENTIFIER_STRING);
            out.writeUTF(stringValue);
            return;
        }
        throw new IOException("Unsupported ordered index identifier: " + identifier);
    }

    private Object readIdentifier(DataInputStream in) throws IOException {
        byte type = in.readByte();
        return switch (type) {
            case IDENTIFIER_RECORD_ID -> new RecordId(in.readLong());
            case IDENTIFIER_STRING -> in.readUTF();
            default -> throw new IOException("Unsupported ordered index identifier type " + type);
        };
    }
}
