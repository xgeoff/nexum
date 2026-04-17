package biz.digitalindustry.db.engine;

import biz.digitalindustry.db.index.ExactMatchIndex;
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
import java.util.LinkedHashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class ExactMatchIndexManager {
    private static final int VERSION = 1;
    private static final byte FIELD_STRING = 1;
    private static final byte FIELD_LONG = 2;
    private static final byte FIELD_DOUBLE = 3;
    private static final byte FIELD_BOOLEAN = 4;
    private static final byte FIELD_REFERENCE = 5;

    private static final byte IDENTIFIER_RECORD_ID = 1;
    private static final byte IDENTIFIER_STRING = 2;

    private final Map<String, ExactMatchIndex<Object>> indexesByNamespace = new LinkedHashMap<>();

    public synchronized boolean ensureNamespace(String namespace, Collection<String> indexedFields) {
        ExactMatchIndex<Object> existing = indexesByNamespace.get(namespace);
        Set<String> requestedFields = new LinkedHashSet<>(indexedFields);
        if (existing != null && existing.fields().equals(requestedFields)) {
            return true;
        }
        indexesByNamespace.put(namespace, new ExactMatchIndex<>(indexedFields));
        return false;
    }

    public synchronized void clearNamespace(String namespace) {
        indexesByNamespace.remove(namespace);
    }

    public synchronized void clear() {
        indexesByNamespace.clear();
    }

    public synchronized boolean hasField(String namespace, String fieldName) {
        ExactMatchIndex<Object> index = indexesByNamespace.get(namespace);
        return index != null && index.hasField(fieldName);
    }

    public synchronized Set<Object> find(String namespace, String fieldName, FieldValue value) {
        ExactMatchIndex<Object> index = indexesByNamespace.get(namespace);
        return index == null ? Set.of() : index.find(fieldName, value);
    }

    public synchronized void add(String namespace, Map<String, FieldValue> fieldValues, Object identifier) {
        ExactMatchIndex<Object> index = indexesByNamespace.get(namespace);
        if (index != null) {
            index.add(fieldValues, identifier);
        }
    }

    public synchronized void remove(String namespace, Map<String, FieldValue> fieldValues, Object identifier) {
        ExactMatchIndex<Object> index = indexesByNamespace.get(namespace);
        if (index != null) {
            index.remove(fieldValues, identifier);
        }
    }

    public synchronized byte[] encode() {
        try {
            ByteArrayOutputStream buffer = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(buffer);
            out.writeInt(VERSION);
            out.writeInt(indexesByNamespace.size());
            for (Map.Entry<String, ExactMatchIndex<Object>> namespaceEntry : indexesByNamespace.entrySet()) {
                out.writeUTF(namespaceEntry.getKey());
                List<String> fields = List.copyOf(namespaceEntry.getValue().fields());
                out.writeInt(fields.size());
                for (String field : fields) {
                    out.writeUTF(field);
                    Map<FieldValue, Set<Object>> snapshot = namespaceEntry.getValue().snapshot(field);
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
            throw new RuntimeException("Failed to encode exact-match index manager state", e);
        }
    }

    public synchronized Set<String> namespaces() {
        return new LinkedHashSet<>(indexesByNamespace.keySet());
    }

    public synchronized ExactMatchIndexManager copy() {
        ExactMatchIndexManager copy = new ExactMatchIndexManager();
        copy.decode(encode());
        return copy;
    }

    public synchronized byte[] encodeNamespace(String namespace) {
        ExactMatchIndex<Object> index = indexesByNamespace.get(namespace);
        if (index == null) {
            return new byte[0];
        }
        try {
            ByteArrayOutputStream buffer = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(buffer);
            List<String> fields = List.copyOf(index.fields());
            out.writeInt(VERSION);
            out.writeInt(fields.size());
            for (String field : fields) {
                out.writeUTF(field);
                Map<FieldValue, Set<Object>> snapshot = index.snapshot(field);
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
            throw new RuntimeException("Failed to encode exact-match index namespace " + namespace, e);
        }
    }

    public synchronized Map<String, Map<FieldValue, Set<Object>>> snapshotNamespace(String namespace) {
        ExactMatchIndex<Object> index = indexesByNamespace.get(namespace);
        Map<String, Map<FieldValue, Set<Object>>> snapshot = new LinkedHashMap<>();
        if (index == null) {
            return snapshot;
        }
        for (String field : index.fields()) {
            snapshot.put(field, index.snapshot(field));
        }
        return snapshot;
    }

    public synchronized void restoreNamespace(String namespace, Map<String, Map<FieldValue, Set<Object>>> fieldSnapshots) {
        ExactMatchIndex<Object> index = new ExactMatchIndex<>(fieldSnapshots.keySet());
        index.restore(fieldSnapshots);
        indexesByNamespace.put(namespace, index);
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
                throw new IOException("Unsupported exact-match index namespace version " + version);
            }
            int fieldCount = in.readInt();
            List<String> fields = new java.util.ArrayList<>();
            Map<String, Map<FieldValue, Set<Object>>> fieldSnapshots = new LinkedHashMap<>();
            for (int fieldIndex = 0; fieldIndex < fieldCount; fieldIndex++) {
                String field = in.readUTF();
                fields.add(field);
                int valueCount = in.readInt();
                Map<FieldValue, Set<Object>> values = new LinkedHashMap<>();
                for (int valueIndex = 0; valueIndex < valueCount; valueIndex++) {
                    FieldValue fieldValue = readFieldValue(in);
                    int idCount = in.readInt();
                    Set<Object> identifiers = new LinkedHashSet<>();
                    for (int idIndex = 0; idIndex < idCount; idIndex++) {
                        identifiers.add(readIdentifier(in));
                    }
                    values.put(fieldValue, identifiers);
                }
                fieldSnapshots.put(field, values);
            }
            ExactMatchIndex<Object> index = new ExactMatchIndex<>(fields);
            index.restore(fieldSnapshots);
            indexesByNamespace.put(namespace, index);
        } catch (IOException e) {
            throw new RuntimeException("Failed to decode exact-match index namespace " + namespace, e);
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
                throw new IOException("Unsupported exact-match index manager version " + version);
            }
            int namespaceCount = in.readInt();
            for (int i = 0; i < namespaceCount; i++) {
                String namespace = in.readUTF();
                int fieldCount = in.readInt();
                List<String> fields = new java.util.ArrayList<>();
                Map<String, Map<FieldValue, Set<Object>>> fieldSnapshots = new LinkedHashMap<>();
                for (int fieldIndex = 0; fieldIndex < fieldCount; fieldIndex++) {
                    String field = in.readUTF();
                    fields.add(field);
                    int valueCount = in.readInt();
                    Map<FieldValue, Set<Object>> values = new LinkedHashMap<>();
                    for (int valueIndex = 0; valueIndex < valueCount; valueIndex++) {
                        FieldValue fieldValue = readFieldValue(in);
                        int idCount = in.readInt();
                        Set<Object> identifiers = new LinkedHashSet<>();
                        for (int idIndex = 0; idIndex < idCount; idIndex++) {
                            identifiers.add(readIdentifier(in));
                        }
                        values.put(fieldValue, identifiers);
                    }
                    fieldSnapshots.put(field, values);
                }
                ExactMatchIndex<Object> index = new ExactMatchIndex<>(fields);
                index.restore(fieldSnapshots);
                indexesByNamespace.put(namespace, index);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to decode exact-match index manager state", e);
        }
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
        throw new IOException("Unsupported indexed field value: " + value);
    }

    private FieldValue readFieldValue(DataInputStream in) throws IOException {
        byte type = in.readByte();
        return switch (type) {
            case FIELD_STRING -> new StringValue(in.readUTF());
            case FIELD_LONG -> new LongValue(in.readLong());
            case FIELD_DOUBLE -> new DoubleValue(in.readDouble());
            case FIELD_BOOLEAN -> new BooleanValue(in.readBoolean());
            case FIELD_REFERENCE -> new ReferenceValue(new RecordId(in.readLong()));
            default -> throw new IOException("Unsupported indexed field value type " + type);
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
        throw new IOException("Unsupported exact-match index identifier: " + identifier);
    }

    private Object readIdentifier(DataInputStream in) throws IOException {
        byte type = in.readByte();
        return switch (type) {
            case IDENTIFIER_RECORD_ID -> new RecordId(in.readLong());
            case IDENTIFIER_STRING -> in.readUTF();
            default -> throw new IOException("Unsupported exact-match index identifier type " + type);
        };
    }
}
