package biz.digitalindustry.db.engine.codec;

import biz.digitalindustry.db.engine.NativeStorageState;
import biz.digitalindustry.db.model.BooleanValue;
import biz.digitalindustry.db.model.DoubleValue;
import biz.digitalindustry.db.model.FieldValue;
import biz.digitalindustry.db.model.ListValue;
import biz.digitalindustry.db.model.LongValue;
import biz.digitalindustry.db.model.NullValue;
import biz.digitalindustry.db.model.Record;
import biz.digitalindustry.db.model.RecordId;
import biz.digitalindustry.db.model.ReferenceValue;
import biz.digitalindustry.db.model.StringValue;
import biz.digitalindustry.db.model.Vector;
import biz.digitalindustry.db.model.VectorValue;
import biz.digitalindustry.db.object.api.ObjectType;
import biz.digitalindustry.db.relational.api.TableDefinition;
import biz.digitalindustry.db.schema.EntityType;
import biz.digitalindustry.db.schema.FieldDefinition;
import biz.digitalindustry.db.schema.IndexDefinition;
import biz.digitalindustry.db.schema.IndexKind;
import biz.digitalindustry.db.schema.SchemaDefinition;
import biz.digitalindustry.db.schema.SchemaKind;
import biz.digitalindustry.db.schema.ValueType;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class StorageSnapshotCodec {
    private static final int VERSION = 4;
    private static final int PRIOR_VERSION = 3;
    private static final int LEGACY_VERSION = 2;

    private StorageSnapshotCodec() {
    }

    public static byte[] encode(NativeStorageState state) {
        try {
            ByteArrayOutputStream buffer = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(buffer);
            out.writeInt(VERSION);
            writeRoot(out, state.root());
            writeSchemas(out, state.schemas());
            out.writeLong(state.nextId());
            out.writeLong(state.lastAppliedBatchId());
            out.writeInt(state.records().size());
            for (Record record : state.records().values()) {
                out.writeLong(record.id().value());
                out.writeUTF(record.type().name());
                out.writeInt(record.fields().size());
                for (Map.Entry<String, FieldValue> entry : record.fields().entrySet()) {
                    out.writeUTF(entry.getKey());
                    writeFieldValue(out, entry.getValue());
                }
            }
            out.flush();
            return buffer.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Failed to encode storage snapshot", e);
        }
    }

    public static NativeStorageState decode(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return NativeStorageState.empty();
        }
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes));
            int version = in.readInt();
            if (version != VERSION && version != PRIOR_VERSION && version != LEGACY_VERSION) {
                throw new IOException("Unsupported snapshot version " + version);
            }

            Object root = readRoot(in);
            Map<String, SchemaDefinition> schemas = readSchemas(in, version);
            long nextId = in.readLong();
            long lastAppliedBatchId = in.readLong();
            int recordCount = in.readInt();
            Map<RecordId, Record> records = new LinkedHashMap<>();
            for (int i = 0; i < recordCount; i++) {
                RecordId recordId = new RecordId(in.readLong());
                String typeName = in.readUTF();
                SchemaDefinition schema = schemas.get(typeName);
                EntityType type = schema != null ? schema.entityType() : null;
                if (type == null) {
                    type = new EntityType(typeName, List.of(), List.of());
                    schemas.put(typeName, SchemaDefinition.of(type, SchemaKind.CORE));
                }
                int fieldCount = in.readInt();
                Map<String, FieldValue> fields = new LinkedHashMap<>();
                for (int j = 0; j < fieldCount; j++) {
                    fields.put(in.readUTF(), readFieldValue(in));
                }
                records.put(recordId, new Record(recordId, type, fields));
            }
            return new NativeStorageState(root, schemas, records, nextId, lastAppliedBatchId);
        } catch (IOException e) {
            throw new RuntimeException("Failed to decode storage snapshot", e);
        }
    }

    private static void writeRoot(DataOutputStream out, Object root) throws IOException {
        out.writeBoolean(root != null);
        if (root != null) {
            writeFieldValue(out, objectToFieldValue(root));
        }
    }

    private static Object readRoot(DataInputStream in) throws IOException {
        return in.readBoolean() ? fieldValueToObject(readFieldValue(in)) : null;
    }

    private static void writeSchemas(DataOutputStream out, Map<String, SchemaDefinition> schemas) throws IOException {
        out.writeInt(schemas.size());
        for (SchemaDefinition schema : schemas.values()) {
            out.writeUTF(schema.name());
            out.writeUTF(schema.kind().name());
            out.writeInt(schema.fields().size());
            for (FieldDefinition field : schema.fields()) {
                out.writeUTF(field.name());
                out.writeUTF(field.type().name());
                out.writeBoolean(field.required());
                out.writeBoolean(field.repeated());
                out.writeInt(field.vectorDimension());
            }
            out.writeInt(schema.indexes().size());
            for (IndexDefinition index : schema.indexes()) {
                out.writeUTF(index.name());
                out.writeUTF(index.kind().name());
                out.writeInt(index.fields().size());
                for (String field : index.fields()) {
                    out.writeUTF(field);
                }
                out.writeInt(index.vectorDimension());
                out.writeBoolean(index.distanceMetric() != null);
                if (index.distanceMetric() != null) {
                    out.writeUTF(index.distanceMetric());
                }
            }
        }
    }

    private static Map<String, SchemaDefinition> readSchemas(DataInputStream in, int version) throws IOException {
        int typeCount = in.readInt();
        Map<String, SchemaDefinition> schemas = new LinkedHashMap<>();
        for (int i = 0; i < typeCount; i++) {
            String name = in.readUTF();
            SchemaKind kind = version >= VERSION ? SchemaKind.valueOf(in.readUTF()) : null;
            int fieldCount = in.readInt();
            List<FieldDefinition> fields = new ArrayList<>(fieldCount);
            for (int j = 0; j < fieldCount; j++) {
                fields.add(new FieldDefinition(
                        in.readUTF(),
                        ValueType.valueOf(in.readUTF()),
                        in.readBoolean(),
                        in.readBoolean(),
                        version >= PRIOR_VERSION ? in.readInt() : 0
                ));
            }
            int indexCount = in.readInt();
            List<IndexDefinition> indexes = new ArrayList<>(indexCount);
            for (int j = 0; j < indexCount; j++) {
                String indexName = in.readUTF();
                IndexKind indexKind = IndexKind.valueOf(in.readUTF());
                int indexFieldCount = in.readInt();
                List<String> indexFields = new ArrayList<>(indexFieldCount);
                for (int k = 0; k < indexFieldCount; k++) {
                    indexFields.add(in.readUTF());
                }
                int vectorDimension = version >= PRIOR_VERSION ? in.readInt() : 0;
                String distanceMetric = version >= PRIOR_VERSION && in.readBoolean() ? in.readUTF() : null;
                indexes.add(new IndexDefinition(indexName, indexKind, indexFields, vectorDimension, distanceMetric));
            }
            if (kind == null) {
                kind = inferLegacySchemaKind(new EntityType(name, fields, indexes));
            }
            schemas.put(name, new SchemaDefinition(name, kind, fields, indexes));
        }
        return schemas;
    }

    private static SchemaKind inferLegacySchemaKind(EntityType entityType) {
        if (TableDefinition.isRelationalEntityType(entityType)) {
            return SchemaKind.RELATIONAL;
        }
        if (ObjectType.isObjectEntityType(entityType)) {
            return SchemaKind.OBJECT;
        }
        if ("Node".equals(entityType.name()) || "Edge".equals(entityType.name())) {
            return SchemaKind.GRAPH;
        }
        return SchemaKind.CORE;
    }

    private static void writeFieldValue(DataOutputStream out, FieldValue value) throws IOException {
        if (value instanceof NullValue) {
            out.writeByte(0);
        } else if (value instanceof StringValue stringValue) {
            out.writeByte(1);
            out.writeUTF(stringValue.value());
        } else if (value instanceof LongValue longValue) {
            out.writeByte(2);
            out.writeLong(longValue.value());
        } else if (value instanceof DoubleValue doubleValue) {
            out.writeByte(3);
            out.writeDouble(doubleValue.value());
        } else if (value instanceof BooleanValue booleanValue) {
            out.writeByte(4);
            out.writeBoolean(booleanValue.value());
        } else if (value instanceof ReferenceValue referenceValue) {
            out.writeByte(5);
            out.writeLong(referenceValue.recordId().value());
        } else if (value instanceof ListValue listValue) {
            out.writeByte(6);
            out.writeInt(listValue.values().size());
            for (FieldValue nested : listValue.values()) {
                writeFieldValue(out, nested);
            }
        } else if (value instanceof VectorValue vectorValue) {
            out.writeByte(7);
            Vector vector = vectorValue.vector();
            out.writeInt(vector.dimension());
            for (float component : vector.values()) {
                out.writeFloat(component);
            }
        } else {
            throw new IOException("Unsupported field value type: " + value.getClass().getName());
        }
    }

    private static FieldValue readFieldValue(DataInputStream in) throws IOException {
        return switch (in.readByte()) {
            case 0 -> NullValue.INSTANCE;
            case 1 -> new StringValue(in.readUTF());
            case 2 -> new LongValue(in.readLong());
            case 3 -> new DoubleValue(in.readDouble());
            case 4 -> new BooleanValue(in.readBoolean());
            case 5 -> new ReferenceValue(new RecordId(in.readLong()));
            case 6 -> {
                int size = in.readInt();
                List<FieldValue> values = new ArrayList<>(size);
                for (int i = 0; i < size; i++) {
                    values.add(readFieldValue(in));
                }
                yield new ListValue(values);
            }
            case 7 -> {
                int dimension = in.readInt();
                float[] values = new float[dimension];
                for (int i = 0; i < dimension; i++) {
                    values[i] = in.readFloat();
                }
                yield new VectorValue(new Vector(values));
            }
            default -> throw new IOException("Unsupported encoded field value");
        };
    }

    private static FieldValue objectToFieldValue(Object value) {
        if (value == null) {
            return NullValue.INSTANCE;
        }
        if (value instanceof String stringValue) {
            return new StringValue(stringValue);
        }
        if (value instanceof Long longValue) {
            return new LongValue(longValue);
        }
        if (value instanceof Integer intValue) {
            return new LongValue(intValue.longValue());
        }
        if (value instanceof Double doubleValue) {
            return new DoubleValue(doubleValue);
        }
        if (value instanceof Float floatValue) {
            return new DoubleValue(floatValue.doubleValue());
        }
        if (value instanceof Boolean booleanValue) {
            return new BooleanValue(booleanValue);
        }
        if (value instanceof Vector vectorValue) {
            return new VectorValue(vectorValue);
        }
        throw new IllegalArgumentException("Unsupported root value type: " + value.getClass().getName());
    }

    private static Object fieldValueToObject(FieldValue value) {
        if (value instanceof NullValue) {
            return null;
        }
        if (value instanceof StringValue stringValue) {
            return stringValue.value();
        }
        if (value instanceof LongValue longValue) {
            return longValue.value();
        }
        if (value instanceof DoubleValue doubleValue) {
            return doubleValue.value();
        }
        if (value instanceof BooleanValue booleanValue) {
            return booleanValue.value();
        }
        if (value instanceof ReferenceValue referenceValue) {
            return referenceValue.recordId();
        }
        if (value instanceof ListValue listValue) {
            List<Object> values = new ArrayList<>(listValue.values().size());
            for (FieldValue nested : listValue.values()) {
                values.add(fieldValueToObject(nested));
            }
            return values;
        }
        if (value instanceof VectorValue vectorValue) {
            return vectorValue.vector();
        }
        throw new IllegalArgumentException("Unsupported root field value type: " + value.getClass().getName());
    }
}
