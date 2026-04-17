package biz.digitalindustry.db.engine.page;

import java.io.Closeable;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

public class PageFile implements Closeable {
    public static final byte[] MAGIC = "PDSTN01".getBytes(StandardCharsets.US_ASCII);
    public static final int HEADER_SIZE = 4096;
    private static final int VERSION = 2;
    private static final int CHECKPOINT_GENERATION_OFFSET = 16;
    private static final int OVERFLOW_CHECKPOINT_OFFSET = 32;
    private static final int SLOT0_DESCRIPTOR_OFFSET = 32;
    private static final int SLOT1_DESCRIPTOR_OFFSET = 48;
    private static final int PAYLOAD_OFFSET = 64;

    private final Path path;
    private final int pageSize;
    private RandomAccessFile file;

    public PageFile(Path path, int pageSize) {
        this.path = path;
        this.pageSize = pageSize;
    }

    public synchronized void open() throws IOException {
        Path parent = path.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        file = new RandomAccessFile(path.toFile(), "rw");
        if (file.length() == 0) {
            writeHeader();
        } else {
            validateHeader();
        }
    }

    public synchronized boolean isOpen() {
        return file != null;
    }

    public int pageSize() {
        return pageSize;
    }

    public Path path() {
        return path;
    }

    public synchronized byte[] readPayload() throws IOException {
        CheckpointCandidate chosen = readLatestValidCheckpoint();
        if (chosen == null) {
            return new byte[0];
        }
        return chosen.payload();
    }

    public synchronized void writePayload(byte[] payload) throws IOException {
        int slotCapacity = slotCapacity();
        if (payload.length > slotCapacity) {
            writeOverflowCheckpoint(payload, reserveNextCheckpointGeneration());
            return;
        }

        SlotDescriptor slot0 = readSlotDescriptor(SLOT0_DESCRIPTOR_OFFSET);
        SlotDescriptor slot1 = readSlotDescriptor(SLOT1_DESCRIPTOR_OFFSET);
        SlotDescriptor current = chooseLatestValidSlot(slot0, slot1);
        boolean useSlot0 = current == null || current.descriptorOffset() == SLOT1_DESCRIPTOR_OFFSET;
        long nextSequence = reserveNextCheckpointGeneration();
        long dataOffset = useSlot0 ? PAYLOAD_OFFSET : PAYLOAD_OFFSET + slotCapacity;
        int descriptorOffset = useSlot0 ? SLOT0_DESCRIPTOR_OFFSET : SLOT1_DESCRIPTOR_OFFSET;

        file.seek(dataOffset);
        file.write(payload);
        int remaining = slotCapacity - payload.length;
        if (remaining > 0) {
            file.write(new byte[remaining]);
        }

        java.util.zip.CRC32 crc32 = new java.util.zip.CRC32();
        crc32.update(payload);
        writeSlotDescriptor(descriptorOffset, nextSequence, payload.length, (int) crc32.getValue());

        if (file.length() < HEADER_SIZE) {
            file.setLength(HEADER_SIZE);
        }
    }

    public synchronized void corruptLatestPayloadByteForTest() throws IOException {
        SlotDescriptor slot0 = readSlotDescriptor(SLOT0_DESCRIPTOR_OFFSET);
        SlotDescriptor slot1 = readSlotDescriptor(SLOT1_DESCRIPTOR_OFFSET);
        CheckpointCandidate chosen = readLatestValidCheckpoint();
        if (chosen == null || chosen.payload().length == 0) {
            corruptOverflowCheckpointByte();
            return;
        }
        if (chosen.slotDescriptor() != null) {
            long bytePos = chosen.slotDescriptor().dataOffset() + chosen.slotDescriptor().length() - 1L;
            file.seek(bytePos);
            int current = file.readUnsignedByte();
            file.seek(bytePos);
            file.writeByte(current ^ 0x01);
        } else {
            corruptOverflowCheckpointByte();
        }
    }

    public synchronized void writeIncompleteSlotCheckpointForTest(byte[] payload) throws IOException {
        int slotCapacity = slotCapacity();
        if (payload.length > slotCapacity) {
            throw new IOException("Payload too large for slot checkpoint helper");
        }

        SlotDescriptor slot0 = readSlotDescriptor(SLOT0_DESCRIPTOR_OFFSET);
        SlotDescriptor slot1 = readSlotDescriptor(SLOT1_DESCRIPTOR_OFFSET);
        SlotDescriptor current = chooseLatestValidSlot(slot0, slot1);
        boolean useSlot0 = current == null || current.descriptorOffset() == SLOT1_DESCRIPTOR_OFFSET;
        reserveNextCheckpointGeneration();

        long dataOffset = useSlot0 ? PAYLOAD_OFFSET : PAYLOAD_OFFSET + slotCapacity;
        file.seek(dataOffset);
        file.write(payload);
        int remaining = slotCapacity - payload.length;
        if (remaining > 0) {
            file.write(new byte[remaining]);
        }
    }

    public synchronized void writeCorruptSlotCheckpointForTest(byte[] payload) throws IOException {
        int slotCapacity = slotCapacity();
        if (payload.length > slotCapacity) {
            throw new IOException("Payload too large for slot checkpoint helper");
        }

        SlotDescriptor slot0 = readSlotDescriptor(SLOT0_DESCRIPTOR_OFFSET);
        SlotDescriptor slot1 = readSlotDescriptor(SLOT1_DESCRIPTOR_OFFSET);
        SlotDescriptor current = chooseLatestValidSlot(slot0, slot1);
        boolean useSlot0 = current == null || current.descriptorOffset() == SLOT1_DESCRIPTOR_OFFSET;
        long generation = reserveNextCheckpointGeneration();
        long dataOffset = useSlot0 ? PAYLOAD_OFFSET : PAYLOAD_OFFSET + slotCapacity;
        int descriptorOffset = useSlot0 ? SLOT0_DESCRIPTOR_OFFSET : SLOT1_DESCRIPTOR_OFFSET;

        file.seek(dataOffset);
        file.write(payload);
        int remaining = slotCapacity - payload.length;
        if (remaining > 0) {
            file.write(new byte[remaining]);
        }

        java.util.zip.CRC32 crc32 = new java.util.zip.CRC32();
        crc32.update(payload);
        writeSlotDescriptor(descriptorOffset, generation, payload.length, (int) crc32.getValue() ^ 0x01);
    }

    public synchronized void clearEntries() throws IOException {
        file.setLength(HEADER_SIZE);
        file.seek(HEADER_SIZE);
    }

    public synchronized void appendEntry(byte[] entry) throws IOException {
        file.seek(file.length());
        file.writeInt(entry.length);
        file.write(entry);
    }

    public synchronized java.util.List<byte[]> readEntries() throws IOException {
        java.util.List<byte[]> entries = new java.util.ArrayList<>();
        long position = HEADER_SIZE;
        while (position < file.length()) {
            file.seek(position);
            int length = file.readInt();
            if (length < 0) {
                throw new IOException("Negative entry length in " + path);
            }
            byte[] entry = new byte[length];
            file.readFully(entry);
            entries.add(entry);
            position += Integer.BYTES + length;
        }
        return entries;
    }

    public synchronized int getPageCount() throws IOException {
        long length = file.length();
        if (length <= HEADER_SIZE) {
            return 0;
        }
        return (int) ((length - HEADER_SIZE) / pageSize);
    }

    public synchronized void setPageCount(int pageCount) throws IOException {
        if (pageCount < 0) {
            throw new IOException("Negative page count");
        }
        file.setLength(HEADER_SIZE + (long) pageCount * pageSize);
    }

    public synchronized int allocatePage() throws IOException {
        int pageNo = getPageCount();
        file.setLength(HEADER_SIZE + (long) (pageNo + 1) * pageSize);
        return pageNo;
    }

    public synchronized void clearPages() throws IOException {
        file.setLength(HEADER_SIZE);
        file.seek(HEADER_SIZE);
    }

    public synchronized void writePage(int pageNo, byte[] payload) throws IOException {
        if (payload.length > pageSize - Integer.BYTES) {
            throw new IOException("Page payload too large for configured page size");
        }
        long position = HEADER_SIZE + (long) pageNo * pageSize;
        file.seek(position);
        file.writeInt(payload.length);
        file.write(payload);
        int remaining = pageSize - Integer.BYTES - payload.length;
        if (remaining > 0) {
            file.write(new byte[remaining]);
        }
    }

    public synchronized byte[] readPage(int pageNo) throws IOException {
        long position = HEADER_SIZE + (long) pageNo * pageSize;
        if (position + Integer.BYTES > file.length()) {
            throw new IOException("Page " + pageNo + " is out of bounds");
        }
        file.seek(position);
        int length = file.readInt();
        if (length < 0 || length > pageSize - Integer.BYTES) {
            throw new IOException("Invalid page payload length " + length + " at page " + pageNo);
        }
        byte[] payload = new byte[length];
        file.readFully(payload);
        return payload;
    }

    private void writeHeader() throws IOException {
        file.seek(0);
        file.write(MAGIC);
        file.writeInt(VERSION);
        file.writeInt(pageSize);
        file.writeLong(1L);
        file.write(new byte[HEADER_SIZE - (MAGIC.length + Integer.BYTES * 2 + Long.BYTES)]);
        if (file.length() < HEADER_SIZE) {
            file.setLength(HEADER_SIZE);
        }
    }

    private void validateHeader() throws IOException {
        file.seek(0);
        byte[] magic = new byte[MAGIC.length];
        file.readFully(magic);
        if (!java.util.Arrays.equals(MAGIC, magic)) {
            throw new IOException("Invalid native storage file header for " + path);
        }
        int version = file.readInt();
        if (version != VERSION) {
            throw new IOException("Unsupported native storage version " + version);
        }
        int storedPageSize = file.readInt();
        if (storedPageSize != pageSize) {
            throw new IOException("Configured page size " + pageSize + " does not match stored page size " + storedPageSize);
        }
        if (readStoredCheckpointGeneration() <= 0) {
            writeStoredCheckpointGeneration(1L);
        }
    }

    private int slotCapacity() {
        return (HEADER_SIZE - PAYLOAD_OFFSET) / 2;
    }

    private SlotDescriptor readSlotDescriptor(int descriptorOffset) throws IOException {
        file.seek(descriptorOffset);
        long sequence = file.readLong();
        int length = file.readInt();
        int checksum = file.readInt();
        long dataOffset = descriptorOffset == SLOT0_DESCRIPTOR_OFFSET ? PAYLOAD_OFFSET : PAYLOAD_OFFSET + slotCapacity();
        return new SlotDescriptor(descriptorOffset, dataOffset, sequence, length, checksum);
    }

    private void writeSlotDescriptor(int descriptorOffset, long sequence, int length, int checksum) throws IOException {
        file.seek(descriptorOffset);
        file.writeLong(sequence);
        file.writeInt(length);
        file.writeInt(checksum);
    }

    private SlotDescriptor chooseLatestValidSlot(SlotDescriptor slot0, SlotDescriptor slot1) throws IOException {
        SlotDescriptor valid0 = isValidSlot(slot0) ? slot0 : null;
        SlotDescriptor valid1 = isValidSlot(slot1) ? slot1 : null;
        if (valid0 == null) {
            return valid1;
        }
        if (valid1 == null) {
            return valid0;
        }
        return valid0.sequence() >= valid1.sequence() ? valid0 : valid1;
    }

    private boolean isValidSlot(SlotDescriptor slot) throws IOException {
        if (slot.sequence() <= 0 || slot.length() < 0 || slot.length() > slotCapacity()) {
            return false;
        }
        if (slot.length() == 0) {
            return true;
        }
        byte[] payload = readSlotPayload(slot);
        java.util.zip.CRC32 crc32 = new java.util.zip.CRC32();
        crc32.update(payload);
        return (int) crc32.getValue() == slot.checksum();
    }

    private byte[] readSlotPayload(SlotDescriptor slot) throws IOException {
        byte[] payload = new byte[slot.length()];
        file.seek(slot.dataOffset());
        file.readFully(payload);
        return payload;
    }

    private byte[] readOverflowCheckpointPayload() throws IOException {
        file.seek(MAGIC.length + Integer.BYTES * 2L);
        int length = file.readInt();
        if (length < 0) {
            throw new IOException("Negative payload length in " + path);
        }
        if (length == 0) {
            return new byte[0];
        }
        if (length > HEADER_SIZE - OVERFLOW_CHECKPOINT_OFFSET) {
            throw new IOException("Overflow checkpoint payload too large in " + path);
        }
        byte[] payload = new byte[length];
        file.seek(OVERFLOW_CHECKPOINT_OFFSET);
        file.readFully(payload);
        return payload;
    }

    private void writeOverflowCheckpoint(byte[] payload, long sequence) throws IOException {
        int storedLength = Long.BYTES + Integer.BYTES + payload.length;
        if (storedLength > HEADER_SIZE - OVERFLOW_CHECKPOINT_OFFSET) {
            throw new IOException("Payload too large for page file header");
        }
        file.seek(MAGIC.length + Integer.BYTES * 2L);
        file.writeInt(storedLength);
        file.seek(OVERFLOW_CHECKPOINT_OFFSET);
        file.writeLong(sequence);
        java.util.zip.CRC32 crc32 = new java.util.zip.CRC32();
        crc32.update(payload);
        file.writeInt((int) crc32.getValue());
        file.write(payload);
        int remaining = HEADER_SIZE - OVERFLOW_CHECKPOINT_OFFSET - storedLength;
        if (remaining > 0) {
            file.write(new byte[remaining]);
        }
        if (file.length() < HEADER_SIZE) {
            file.setLength(HEADER_SIZE);
        }
    }

    private void clearOverflowCheckpoint() throws IOException {
        file.seek(MAGIC.length + Integer.BYTES * 2L);
        file.writeInt(0);
    }

    private void clearSlotDescriptors() throws IOException {
        writeSlotDescriptor(SLOT0_DESCRIPTOR_OFFSET, 0L, 0, 0);
        writeSlotDescriptor(SLOT1_DESCRIPTOR_OFFSET, 0L, 0, 0);
    }

    private void corruptOverflowCheckpointByte() throws IOException {
        CheckpointCandidate overflow = readOverflowCheckpoint();
        if (overflow == null || overflow.payload().length == 0) {
            throw new IOException("No overflow checkpoint payload available");
        }
        long position = OVERFLOW_CHECKPOINT_OFFSET + Long.BYTES + Integer.BYTES + overflow.payload().length - 1L;
        file.seek(position);
        int current = file.readUnsignedByte();
        file.seek(position);
        file.writeByte(current ^ 0x01);
    }

    private CheckpointCandidate readLatestValidCheckpoint() throws IOException {
        SlotDescriptor slot0 = readSlotDescriptor(SLOT0_DESCRIPTOR_OFFSET);
        SlotDescriptor slot1 = readSlotDescriptor(SLOT1_DESCRIPTOR_OFFSET);
        SlotDescriptor chosenSlot = chooseLatestValidSlot(slot0, slot1);
        CheckpointCandidate slotCandidate = chosenSlot == null ? null : new CheckpointCandidate(chosenSlot.sequence(), readSlotPayload(chosenSlot), chosenSlot);
        CheckpointCandidate overflowCandidate = readOverflowCheckpoint();
        if (slotCandidate == null) {
            return overflowCandidate;
        }
        if (overflowCandidate == null) {
            return slotCandidate;
        }
        return slotCandidate.sequence() >= overflowCandidate.sequence() ? slotCandidate : overflowCandidate;
    }

    private CheckpointCandidate readOverflowCheckpoint() throws IOException {
        byte[] stored = readOverflowCheckpointPayload();
        if (stored.length == 0) {
            return null;
        }
        if (stored.length < Long.BYTES + Integer.BYTES) {
            return null;
        }
        java.nio.ByteBuffer buffer = java.nio.ByteBuffer.wrap(stored);
        long sequence = buffer.getLong();
        int expectedChecksum = buffer.getInt();
        byte[] payload = new byte[buffer.remaining()];
        buffer.get(payload);
        java.util.zip.CRC32 crc32 = new java.util.zip.CRC32();
        crc32.update(payload);
        if ((int) crc32.getValue() != expectedChecksum) {
            return null;
        }
        return new CheckpointCandidate(sequence, payload, null);
    }

    private long reserveNextCheckpointGeneration() throws IOException {
        long current = readStoredCheckpointGeneration();
        long next = current <= 0 ? 1L : current;
        writeStoredCheckpointGeneration(next + 1L);
        return next;
    }

    private long readStoredCheckpointGeneration() throws IOException {
        file.seek(CHECKPOINT_GENERATION_OFFSET);
        return file.readLong();
    }

    private void writeStoredCheckpointGeneration(long generation) throws IOException {
        file.seek(CHECKPOINT_GENERATION_OFFSET);
        file.writeLong(generation);
    }

    private record SlotDescriptor(int descriptorOffset, long dataOffset, long sequence, int length, int checksum) {
    }

    private record CheckpointCandidate(long sequence, byte[] payload, SlotDescriptor slotDescriptor) {
    }

    @Override
    public synchronized void close() throws IOException {
        if (file != null) {
            file.close();
            file = null;
        }
    }
}
