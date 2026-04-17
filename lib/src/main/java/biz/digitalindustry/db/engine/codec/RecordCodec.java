package biz.digitalindustry.db.engine.codec;

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
import biz.digitalindustry.db.schema.EntityType;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public final class RecordCodec {
    private RecordCodec() {
    }

    public static byte[] encode(Record record) {
        try {
            ByteArrayOutputStream buffer = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(buffer);
            out.writeLong(record.id().value());
            out.writeUTF(record.type().name());
            out.writeInt(record.fields().size());
            for (Map.Entry<String, FieldValue> entry : record.fields().entrySet()) {
                out.writeUTF(entry.getKey());
                writeFieldValue(out, entry.getValue());
            }
            out.flush();
            return buffer.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Failed to encode record", e);
        }
    }

    public static Record decode(byte[] bytes, Function<String, EntityType> entityTypeResolver) {
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes));
            RecordId id = new RecordId(in.readLong());
            String typeName = in.readUTF();
            EntityType type = entityTypeResolver.apply(typeName);
            if (type == null) {
                type = new EntityType(typeName, List.of(), List.of());
            }
            int fieldCount = in.readInt();
            Map<String, FieldValue> fields = new LinkedHashMap<>();
            for (int i = 0; i < fieldCount; i++) {
                fields.put(in.readUTF(), readFieldValue(in));
            }
            return new Record(id, type, fields);
        } catch (IOException e) {
            throw new RuntimeException("Failed to decode record", e);
        }
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
}
