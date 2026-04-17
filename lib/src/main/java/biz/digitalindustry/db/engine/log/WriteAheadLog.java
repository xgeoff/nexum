package biz.digitalindustry.db.engine.log;

import java.io.Closeable;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class WriteAheadLog implements Closeable {
    private static final byte[] MAGIC = "PDSTWAL1".getBytes(StandardCharsets.US_ASCII);
    public static final byte BEGIN = 1;
    public static final byte UPSERT = 2;
    public static final byte DELETE = 3;
    public static final byte METADATA = 4;
    public static final byte COMMIT = 5;
    public static final byte ROLLBACK = 6;
    public static final byte RECORD_PAGE_COUNT = 7;
    public static final byte RECORD_SLOT_INDEX = 10;
    public static final byte RECORD_ALLOCATOR_STATE = 11;
    public static final byte RECORD_PAGE_DIFF = 12;
    public static final byte EXACT_INDEX_NAMESPACE = 14;
    public static final byte EXACT_INDEX_ADD = 15;
    public static final byte EXACT_INDEX_REMOVE = 16;
    public static final byte ORDERED_INDEX_NAMESPACE = 17;
    public static final byte ORDERED_INDEX_ADD = 18;
    public static final byte ORDERED_INDEX_REMOVE = 19;
    public static final byte VECTOR_INDEX_NAMESPACE = 20;
    public static final byte VECTOR_INDEX_ADD = 21;
    public static final byte VECTOR_INDEX_REMOVE = 22;

    private final Path path;
    private RandomAccessFile file;

    public WriteAheadLog(Path path) {
        this.path = path;
    }

    public synchronized void open() throws IOException {
        Path parent = path.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        file = new RandomAccessFile(path.toFile(), "rw");
        if (file.length() == 0) {
            file.write(MAGIC);
        } else {
            byte[] magic = new byte[MAGIC.length];
            file.seek(0);
            file.readFully(magic);
            if (!java.util.Arrays.equals(MAGIC, magic)) {
                throw new IOException("Invalid WAL header for " + path);
            }
        }
        file.seek(file.length());
    }

    public synchronized boolean isOpen() {
        return file != null;
    }

    public synchronized void append(String entry) {
        append((byte) 0, entry.getBytes(StandardCharsets.UTF_8));
    }

    public synchronized void append(byte type, byte[] payload) {
        if (file == null) {
            throw new IllegalStateException("WAL is not open");
        }
        try {
            file.writeByte(type);
            file.writeInt(payload.length);
            file.write(payload);
        } catch (IOException e) {
            throw new RuntimeException("Failed to append WAL entry", e);
        }
    }

    public synchronized List<Entry> readEntries() {
        if (file == null) {
            throw new IllegalStateException("WAL is not open");
        }
        try {
            List<Entry> entries = new ArrayList<>();
            long previousPosition = file.getFilePointer();
            file.seek(MAGIC.length);
            while (file.getFilePointer() < file.length()) {
                byte type = file.readByte();
                int length = file.readInt();
                byte[] payload = new byte[length];
                file.readFully(payload);
                entries.add(new Entry(type, payload));
            }
            file.seek(previousPosition);
            return entries;
        } catch (IOException e) {
            throw new RuntimeException("Failed to read WAL entries", e);
        }
    }

    public Path path() {
        return path;
    }

    public synchronized long sizeBytes() {
        if (file == null) {
            throw new IllegalStateException("WAL is not open");
        }
        try {
            return file.length();
        } catch (IOException e) {
            throw new RuntimeException("Failed to read WAL size", e);
        }
    }

    public synchronized void clear() {
        if (file == null) {
            throw new IllegalStateException("WAL is not open");
        }
        try {
            file.setLength(0);
            file.seek(0);
            file.write(MAGIC);
            file.seek(file.length());
        } catch (IOException e) {
            throw new RuntimeException("Failed to clear WAL", e);
        }
    }

    public synchronized void corruptLastEntryPayloadByte(byte targetType) {
        if (file == null) {
            throw new IllegalStateException("WAL is not open");
        }
        try {
            long previousPosition = file.getFilePointer();
            long position = MAGIC.length;
            long targetPayloadPos = -1;
            int targetPayloadLen = -1;
            while (position < file.length()) {
                file.seek(position);
                byte type = file.readByte();
                int length = file.readInt();
                long payloadPos = file.getFilePointer();
                if (type == targetType && length > 0) {
                    targetPayloadPos = payloadPos;
                    targetPayloadLen = length;
                }
                position = payloadPos + length;
            }
            if (targetPayloadPos < 0 || targetPayloadLen <= 0) {
                throw new IOException("No WAL entry found for type " + targetType);
            }
            long bytePos = targetPayloadPos + targetPayloadLen - 1L;
            file.seek(bytePos);
            int current = file.readUnsignedByte();
            file.seek(bytePos);
            file.writeByte(current ^ 0x01);
            file.seek(previousPosition);
        } catch (IOException e) {
            throw new RuntimeException("Failed to corrupt WAL entry payload", e);
        }
    }

    @Override
    public synchronized void close() throws IOException {
        if (file != null) {
            file.close();
            file = null;
        }
    }

    public record Entry(byte type, byte[] payload) {
    }
}
