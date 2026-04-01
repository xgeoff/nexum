package biz.digitalindustry.storage.store;

import biz.digitalindustry.storage.codec.RecordCodec;
import biz.digitalindustry.storage.log.WriteAheadLog;
import biz.digitalindustry.storage.model.Record;
import biz.digitalindustry.storage.model.RecordId;
import biz.digitalindustry.storage.page.PageFile;
import biz.digitalindustry.storage.schema.EntityType;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

public class PageBackedRecordStore implements ManagedRecordStore, Closeable {
    private static final int METADATA_MAGIC = 0x5253544d;
    private static final int METADATA_VERSION = 1;

    @FunctionalInterface
    public interface MutationListener {
        void onMutation(byte operationType, Record beforeRecord, Record afterRecord, RecordId recordId);
    }

    private final PageFile pageFile;
    private final Function<String, EntityType> entityTypeResolver;
    private final MutationListener mutationListener;
    private final AtomicLong nextId = new AtomicLong(1);
    private final Map<RecordId, Record> records = new LinkedHashMap<>();
    private final Map<RecordId, PageSlot> recordSlots = new LinkedHashMap<>();
    private final Map<Integer, SlottedPage> pages = new LinkedHashMap<>();
    private final Map<Integer, AllocatorPageState> allocatorState = new LinkedHashMap<>();
    private final Set<Integer> dirtyPages = new LinkedHashSet<>();
    private final Map<Integer, SlottedPage> dirtyPageBeforeImages = new LinkedHashMap<>();
    private boolean autoFlush = true;
    private boolean metadataDirty;
    private boolean pageCountDirty;
    private long metadataGeneration = 1L;

    public PageBackedRecordStore(Path path, int pageSize, Function<String, EntityType> entityTypeResolver) {
        this(path, pageSize, entityTypeResolver, null);
    }

    public PageBackedRecordStore(
            Path path,
            int pageSize,
            Function<String, EntityType> entityTypeResolver,
            MutationListener mutationListener
    ) {
        this.pageFile = new PageFile(path, pageSize);
        this.entityTypeResolver = entityTypeResolver;
        this.mutationListener = mutationListener;
    }

    public synchronized void open() {
        try {
            pageFile.open();
            loadFromDisk();
        } catch (IOException e) {
            throw new RuntimeException("Failed to open page-backed record store at " + pageFile.path(), e);
        }
    }

    @Override
    public synchronized Record create(EntityType type, Record record) {
        RecordId id = record.id() == null ? new RecordId(nextId.getAndIncrement()) : record.id();
        Record stored = new Record(id, type, record.fields());
        nextId.set(Math.max(nextId.get(), id.value() + 1));
        storeUpsert(stored);
        notifyMutation(WriteAheadLog.UPSERT, null, stored, id);
        return stored;
    }

    @Override
    public synchronized Record get(RecordId id) {
        return records.get(id);
    }

    @Override
    public synchronized Record update(Record record) {
        if (record.id() == null || !records.containsKey(record.id())) {
            throw new IllegalArgumentException("Unknown record id: " + (record.id() == null ? "null" : record.id().value()));
        }
        Record previous = records.get(record.id());
        storeUpsert(record);
        notifyMutation(WriteAheadLog.UPSERT, previous, record, record.id());
        return record;
    }

    @Override
    public synchronized boolean delete(RecordId id) {
        Record previous = records.get(id);
        boolean deleted = deleteInternal(id);
        if (deleted) {
            notifyMutation(WriteAheadLog.DELETE, previous, null, id);
        }
        return deleted;
    }

    @Override
    public synchronized Iterable<Record> scan(EntityType type) {
        List<Record> matches = new ArrayList<>();
        for (Record record : records.values()) {
            if (record.type().name().equals(type.name())) {
                matches.add(record);
            }
        }
        return matches;
    }

    public synchronized Map<RecordId, Record> snapshot() {
        return new LinkedHashMap<>(records);
    }

    public synchronized long nextId() {
        return nextId.get();
    }

    public synchronized void replaceWithSnapshot(Map<RecordId, Record> snapshot, long restoredNextId) {
        records.clear();
        records.putAll(snapshot);
        nextId.set(restoredNextId);
        recordSlots.clear();
        pages.clear();
        allocatorState.clear();
        clearDirtyTracking();
        try {
            pageFile.clearPages();
            for (Record record : records.values()) {
                PageSlot slot = allocateSlot(RecordCodec.encode(record));
                recordSlots.put(record.id(), slot);
                refreshAllocatorState(slot.pageNo());
                writePageIfNeeded(slot.pageNo());
            }
            persistMetadataIfNeeded();
            if (autoFlush) {
                clearDirtyTracking();
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to rewrite page-backed record store", e);
        }
    }

    public Path path() {
        return pageFile.path();
    }

    public synchronized void recoverUpsert(Record record) {
        nextId.set(Math.max(nextId.get(), record.id().value() + 1));
        storeUpsert(record);
    }

    public synchronized void recoverDelete(RecordId id) {
        deleteInternal(id);
    }

    public synchronized void setNextId(long restoredNextId) {
        nextId.set(restoredNextId);
        metadataDirty = true;
        persistMetadataIfNeeded();
        if (autoFlush) {
            clearDirtyTracking();
        }
    }

    public synchronized void setAutoFlush(boolean autoFlush) {
        this.autoFlush = autoFlush;
    }

    public synchronized void flush() {
        try {
            flushToDisk();
        } catch (IOException e) {
            throw new RuntimeException("Failed to flush page-backed record store", e);
        }
    }

    public synchronized int currentPageCount() {
        return pages.isEmpty() ? 0 : (pages.keySet().stream().mapToInt(Integer::intValue).max().orElse(-1) + 1);
    }

    public synchronized byte[] encodedMetadata() {
        try {
            return encodeMetadataEnvelope(metadataGeneration, encodeMetadataBody());
        } catch (IOException e) {
            throw new RuntimeException("Failed to encode record store metadata", e);
        }
    }

    public synchronized void corruptLatestMetadataByteForTest() throws IOException {
        pageFile.corruptLatestPayloadByteForTest();
    }

    public synchronized void writeIncompleteMetadataCheckpointForTest() throws IOException {
        pageFile.writeIncompleteSlotCheckpointForTest(encodeMetadataEnvelope(metadataGeneration + 1L, encodeMetadataBody()));
    }

    public synchronized byte[] encodedRecordSlotIndex() {
        return new byte[0];
    }

    public synchronized byte[] encodedAllocatorState() {
        return new byte[0];
    }

    public synchronized Map<Integer, byte[]> encodedPages() {
        Map<Integer, byte[]> encodedPages = new LinkedHashMap<>();
        for (Map.Entry<Integer, SlottedPage> entry : pages.entrySet()) {
            encodedPages.put(entry.getKey(), entry.getValue().encode());
        }
        return encodedPages;
    }

    public synchronized Map<Integer, byte[]> encodedDirtyPages() {
        Map<Integer, byte[]> encodedPages = new LinkedHashMap<>();
        for (Integer pageNo : dirtyPages) {
            SlottedPage page = pages.get(pageNo);
            if (page != null) {
                encodedPages.put(pageNo, page.encode());
            }
        }
        return encodedPages;
    }

    public synchronized Map<Integer, byte[]> encodedDirtyPageDiffs() {
        Map<Integer, byte[]> diffs = new LinkedHashMap<>();
        for (Integer pageNo : dirtyPages) {
            SlottedPage before = dirtyPageBeforeImages.getOrDefault(pageNo, new SlottedPage());
            SlottedPage after = pages.get(pageNo);
            if (after != null) {
                diffs.put(pageNo, SlottedPage.encodeDiff(before, after));
            }
        }
        return diffs;
    }

    public synchronized void applyRecoveredPageState(int pageCount, byte[] metadataPayload, Map<Integer, byte[]> pagePayloads) {
        try {
            pageFile.setPageCount(pageCount);
            pageFile.writePayload(metadataPayload);
            for (Map.Entry<Integer, byte[]> entry : pagePayloads.entrySet()) {
                pageFile.writePage(entry.getKey(), entry.getValue());
            }
            loadFromDisk();
        } catch (IOException e) {
            throw new RuntimeException("Failed to apply recovered record store page state", e);
        }
    }

    public synchronized void applyRecoveredPageState(
            int pageCount,
            byte[] recordSlotIndexPayload,
            byte[] allocatorStatePayload,
            Map<Integer, byte[]> pagePayloads
    ) {
        try {
            pageFile.setPageCount(pageCount);
            pageFile.writePayload(encodeMetadataEnvelope(metadataGeneration + 1L, encodeMetadataBody()));
            for (Map.Entry<Integer, byte[]> entry : pagePayloads.entrySet()) {
                pageFile.writePage(entry.getKey(), entry.getValue());
            }
            loadFromDisk();
        } catch (IOException e) {
            throw new RuntimeException("Failed to apply recovered record store page state", e);
        }
    }

    public synchronized void applyRecoveredPageDiffState(
            int pageCount,
            byte[] recordSlotIndexPayload,
            byte[] allocatorStatePayload,
            Map<Integer, byte[]> pageDiffPayloads
    ) {
        try {
            pageFile.setPageCount(pageCount);
            pageFile.writePayload(encodeMetadataEnvelope(metadataGeneration + 1L, encodeMetadataBody()));
            for (Map.Entry<Integer, byte[]> entry : pageDiffPayloads.entrySet()) {
                byte[] currentPayload = pageFile.readPage(entry.getKey());
                SlottedPage currentPage = currentPayload.length == 0 ? new SlottedPage() : SlottedPage.decode(currentPayload);
                SlottedPage updatedPage = SlottedPage.applyDiff(currentPage, entry.getValue());
                pageFile.writePage(entry.getKey(), updatedPage.encode());
            }
            loadFromDisk();
        } catch (IOException e) {
            throw new RuntimeException("Failed to apply recovered record store page diffs", e);
        }
    }

    @Override
    public synchronized void close() throws IOException {
        pageFile.close();
    }

    private void loadFromDisk() throws IOException {
        records.clear();
        recordSlots.clear();
        pages.clear();
        allocatorState.clear();

        Metadata metadata = readMetadata();
        nextId.set(metadata.nextId);
        metadataGeneration = metadata.generation;

        for (int pageNo = 0; pageNo < pageFile.getPageCount(); pageNo++) {
            byte[] payload = pageFile.readPage(pageNo);
            SlottedPage page = payload.length == 0 ? new SlottedPage() : SlottedPage.decode(payload);
            pages.put(pageNo, page);
            allocatorState.put(pageNo, AllocatorPageState.fromPage(pageNo, page, pageFile.pageSize()));
        }

        for (Map.Entry<Integer, SlottedPage> pageEntry : pages.entrySet()) {
            for (int slotIndex = 0; slotIndex < pageEntry.getValue().slotCount(); slotIndex++) {
                byte[] payload = pageEntry.getValue().get(slotIndex);
                if (payload == null) {
                    continue;
                }
                Record record = RecordCodec.decode(payload, entityTypeResolver);
                records.put(record.id(), record);
                recordSlots.put(record.id(), new PageSlot(pageEntry.getKey(), slotIndex));
            }
        }
        clearDirtyTracking();
    }

    private PageSlot allocateSlot(byte[] encoded) {
        int requiredBytes = encoded.length + Integer.BYTES;
        for (Map.Entry<Integer, AllocatorPageState> entry : allocatorState.entrySet()) {
            if (!entry.getValue().canFit(requiredBytes)) {
                continue;
            }
            SlottedPage page = pages.get(entry.getKey());
            if (page == null) {
                continue;
            }
            markPageDirty(entry.getKey());
            int slotIndex = page.insert(encoded, pageFile.pageSize());
            if (slotIndex >= 0) {
                refreshAllocatorState(entry.getKey());
                return new PageSlot(entry.getKey(), slotIndex);
            }
        }
        for (Map.Entry<Integer, SlottedPage> entry : pages.entrySet()) {
            markPageDirty(entry.getKey());
            int slotIndex = entry.getValue().insert(encoded, pageFile.pageSize());
            if (slotIndex >= 0) {
                refreshAllocatorState(entry.getKey());
                return new PageSlot(entry.getKey(), slotIndex);
            }
        }
        try {
            int pageNo = pageFile.allocatePage();
            SlottedPage page = new SlottedPage();
            markPageDirty(pageNo, new SlottedPage());
            int slotIndex = page.insert(encoded, pageFile.pageSize());
            pages.put(pageNo, page);
            allocatorState.put(pageNo, AllocatorPageState.fromPage(pageNo, page, pageFile.pageSize()));
            pageCountDirty = true;
            metadataDirty = true;
            dirtyPages.add(pageNo);
            return new PageSlot(pageNo, slotIndex);
        } catch (IOException e) {
            throw new RuntimeException("Failed to allocate record slot", e);
        }
    }

    private void writePage(int pageNo) {
        try {
            pageFile.writePage(pageNo, pages.get(pageNo).encode());
        } catch (IOException e) {
            throw new RuntimeException("Failed to write slotted page", e);
        }
    }

    private void writePageIfNeeded(int pageNo) {
        if (autoFlush) {
            writePage(pageNo);
        }
    }

    private Metadata readMetadata() throws IOException {
        byte[] payload = pageFile.readPayload();
        if (payload.length == 0) {
            return new Metadata(1L, 1L);
        }
        DataInputStream header = new DataInputStream(new ByteArrayInputStream(payload));
        if (payload.length >= Integer.BYTES * 3 + Long.BYTES && header.readInt() == METADATA_MAGIC) {
            int version = header.readInt();
            if (version != METADATA_VERSION) {
                throw new IOException("Unsupported record store metadata version " + version);
            }
            long generation = header.readLong();
            int expectedChecksum = header.readInt();
            byte[] body = new byte[header.available()];
            header.readFully(body);
            java.util.zip.CRC32 crc32 = new java.util.zip.CRC32();
            crc32.update(body);
            if ((int) crc32.getValue() != expectedChecksum) {
                throw new IOException("Invalid record store metadata checksum");
            }
            return decodeMetadataBody(body, generation);
        }
        return decodeMetadataBody(payload, 1L);
    }

    private void persistMetadata() {
        try {
            metadataGeneration++;
            pageFile.writePayload(encodeMetadataEnvelope(metadataGeneration, encodeMetadataBody()));
        } catch (IOException e) {
            throw new RuntimeException("Failed to persist record store metadata", e);
        }
    }

    private void persistMetadataIfNeeded() {
        if (autoFlush) {
            persistMetadata();
        }
    }

    private record Metadata(long nextId, long generation) {
    }

    private record PageSlot(int pageNo, int slotIndex) {
    }

    private void storeUpsert(Record record) {
        byte[] encoded = RecordCodec.encode(record);
        PageSlot currentSlot = recordSlots.get(record.id());
        if (currentSlot == null) {
            records.put(record.id(), record);
            PageSlot slot = allocateSlot(encoded);
            recordSlots.put(record.id(), slot);
            refreshAllocatorState(slot.pageNo());
            writePageIfNeeded(slot.pageNo());
            persistMetadataIfNeeded();
            if (autoFlush) {
                clearDirtyTracking();
            }
            return;
        }

        SlottedPage currentPage = pages.get(currentSlot.pageNo());
        if (currentPage.canFitInSlot(currentSlot.slotIndex(), encoded, pageFile.pageSize())) {
            markPageDirty(currentSlot.pageNo());
            currentPage.set(currentSlot.slotIndex(), encoded);
            refreshAllocatorState(currentSlot.pageNo());
            writePageIfNeeded(currentSlot.pageNo());
        } else {
            markPageDirty(currentSlot.pageNo());
            currentPage.delete(currentSlot.slotIndex());
            refreshAllocatorState(currentSlot.pageNo());
            writePageIfNeeded(currentSlot.pageNo());
            PageSlot newSlot = allocateSlot(encoded);
            recordSlots.put(record.id(), newSlot);
            refreshAllocatorState(newSlot.pageNo());
            writePageIfNeeded(newSlot.pageNo());
        }
        records.put(record.id(), record);
        persistMetadataIfNeeded();
        if (autoFlush) {
            clearDirtyTracking();
        }
    }

    private boolean deleteInternal(RecordId id) {
        if (records.remove(id) == null) {
            return false;
        }
        PageSlot slot = recordSlots.remove(id);
        if (slot != null) {
            SlottedPage page = pages.get(slot.pageNo());
            markPageDirty(slot.pageNo());
            page.delete(slot.slotIndex());
            refreshAllocatorState(slot.pageNo());
            writePageIfNeeded(slot.pageNo());
        }
        persistMetadataIfNeeded();
        if (autoFlush) {
            clearDirtyTracking();
        }
        return true;
    }

    private void notifyMutation(byte operationType, Record beforeRecord, Record afterRecord, RecordId recordId) {
        if (mutationListener != null) {
            mutationListener.onMutation(operationType, beforeRecord, afterRecord, recordId);
        }
    }

    private byte[] encodeMetadataBody() throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(buffer);
        out.writeLong(nextId.get());
        out.flush();
        return buffer.toByteArray();
    }

    private byte[] encodeMetadataEnvelope(long generation, byte[] body) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(buffer);
        out.writeInt(METADATA_MAGIC);
        out.writeInt(METADATA_VERSION);
        out.writeLong(generation);
        java.util.zip.CRC32 crc32 = new java.util.zip.CRC32();
        crc32.update(body);
        out.writeInt((int) crc32.getValue());
        out.write(body);
        out.flush();
        return buffer.toByteArray();
    }

    private Metadata decodeMetadataBody(byte[] body, long generation) throws IOException {
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(body));
        long nextId = in.readLong();
        return new Metadata(nextId, generation);
    }

    private void flushToDisk() throws IOException {
        if (pageCountDirty) {
            pageFile.setPageCount(currentPageCount());
        }
        for (Integer pageNo : dirtyPages) {
            SlottedPage page = pages.get(pageNo);
            if (page != null) {
                pageFile.writePage(pageNo, page.encode());
            }
        }
        if (metadataDirty) {
            persistMetadata();
        }
        clearDirtyTracking();
    }

    private void clearDirtyTracking() {
        dirtyPages.clear();
        dirtyPageBeforeImages.clear();
        metadataDirty = false;
        pageCountDirty = false;
    }

    private void refreshAllocatorState(int pageNo) {
        SlottedPage page = pages.get(pageNo);
        if (page != null) {
            allocatorState.put(pageNo, AllocatorPageState.fromPage(pageNo, page, pageFile.pageSize()));
            metadataDirty = true;
            dirtyPages.add(pageNo);
        }
    }

    private void markPageDirty(int pageNo) {
        SlottedPage page = pages.get(pageNo);
        if (page != null) {
            markPageDirty(pageNo, page.copy());
        }
    }

    private void markPageDirty(int pageNo, SlottedPage beforeImage) {
        if (!dirtyPageBeforeImages.containsKey(pageNo)) {
            dirtyPageBeforeImages.put(pageNo, beforeImage);
        }
        dirtyPages.add(pageNo);
    }

    private record AllocatorPageState(int pageNo, int freeBytes, int slotCount, boolean hasReusableSlot) {
        static AllocatorPageState fromPage(int pageNo, SlottedPage page, int pageSize) {
            return new AllocatorPageState(pageNo, page.freeBytes(pageSize), page.slotCount(), page.hasReusableSlot());
        }

        boolean canFit(int requiredBytes) {
            return freeBytes >= requiredBytes || hasReusableSlot;
        }
    }

    private static final class SlottedPage {
        private final List<byte[]> slots = new ArrayList<>();

        int insert(byte[] payload, int pageSize) {
            for (int i = 0; i < slots.size(); i++) {
                if (slots.get(i) == null && canFitInSlot(i, payload, pageSize)) {
                    slots.set(i, payload);
                    return i;
                }
            }
            slots.add(payload);
            if (encodedSize() > maxPayload(pageSize)) {
                slots.remove(slots.size() - 1);
                return -1;
            }
            return slots.size() - 1;
        }

        boolean canFitInSlot(int slotIndex, byte[] payload, int pageSize) {
            byte[] existing = slots.get(slotIndex);
            int currentSize = encodedSize();
            int delta = payload.length - (existing == null ? 0 : existing.length);
            return currentSize + delta <= maxPayload(pageSize);
        }

        void set(int slotIndex, byte[] payload) {
            slots.set(slotIndex, payload);
        }

        void delete(int slotIndex) {
            slots.set(slotIndex, null);
        }

        byte[] get(int slotIndex) {
            return slotIndex >= 0 && slotIndex < slots.size() ? slots.get(slotIndex) : null;
        }

        SlottedPage copy() {
            SlottedPage copy = new SlottedPage();
            for (byte[] slot : slots) {
                copy.slots.add(slot == null ? null : java.util.Arrays.copyOf(slot, slot.length));
            }
            return copy;
        }

        int slotCount() {
            return slots.size();
        }

        boolean hasReusableSlot() {
            for (byte[] slot : slots) {
                if (slot == null) {
                    return true;
                }
            }
            return false;
        }

        int freeBytes(int pageSize) {
            return maxPayload(pageSize) - encodedSize();
        }

        byte[] encode() {
            try {
                ByteArrayOutputStream buffer = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(buffer);
                out.writeInt(slots.size());
                for (byte[] slot : slots) {
                    out.writeInt(slot == null ? -1 : slot.length);
                    if (slot != null) {
                        out.write(slot);
                    }
                }
                out.flush();
                return buffer.toByteArray();
            } catch (IOException e) {
                throw new RuntimeException("Failed to encode slotted page", e);
            }
        }

        static SlottedPage decode(byte[] payload) {
            try {
                SlottedPage page = new SlottedPage();
                DataInputStream in = new DataInputStream(new ByteArrayInputStream(payload));
                int slotCount = in.readInt();
                for (int i = 0; i < slotCount; i++) {
                    int length = in.readInt();
                    if (length < 0) {
                        page.slots.add(null);
                    } else {
                        byte[] slot = new byte[length];
                        in.readFully(slot);
                        page.slots.add(slot);
                    }
                }
                return page;
            } catch (IOException e) {
                throw new RuntimeException("Failed to decode slotted page", e);
            }
        }

        static byte[] encodeDiff(SlottedPage before, SlottedPage after) {
            try {
                byte[] beforeBytes = before.encode();
                byte[] afterBytes = after.encode();
                ByteArrayOutputStream buffer = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(buffer);
                List<ByteRangePatch> patches = new ArrayList<>();
                int maxLength = afterBytes.length;
                int index = 0;
                while (index < maxLength) {
                    byte beforeByte = index < beforeBytes.length ? beforeBytes[index] : 0;
                    byte afterByte = afterBytes[index];
                    if (beforeByte == afterByte) {
                        index++;
                        continue;
                    }
                    int start = index;
                    ByteArrayOutputStream patchBytes = new ByteArrayOutputStream();
                    while (index < maxLength) {
                        beforeByte = index < beforeBytes.length ? beforeBytes[index] : 0;
                        afterByte = afterBytes[index];
                        if (beforeByte == afterByte) {
                            break;
                        }
                        patchBytes.write(afterBytes[index]);
                        index++;
                    }
                    patches.add(new ByteRangePatch(start, patchBytes.toByteArray()));
                }
                out.writeInt(afterBytes.length);
                out.writeInt(patches.size());
                for (ByteRangePatch patch : patches) {
                    out.writeInt(patch.start());
                    out.writeInt(patch.bytes().length);
                    out.write(patch.bytes());
                }
                out.flush();
                return buffer.toByteArray();
            } catch (IOException e) {
                throw new RuntimeException("Failed to encode slotted page diff", e);
            }
        }

        static SlottedPage applyDiff(SlottedPage base, byte[] diffPayload) {
            try {
                byte[] baseBytes = base.encode();
                DataInputStream in = new DataInputStream(new ByteArrayInputStream(diffPayload));
                int finalLength = in.readInt();
                byte[] result = java.util.Arrays.copyOf(baseBytes, finalLength);
                int changeCount = in.readInt();
                for (int i = 0; i < changeCount; i++) {
                    int start = in.readInt();
                    int length = in.readInt();
                    byte[] patch = new byte[length];
                    in.readFully(patch);
                    System.arraycopy(patch, 0, result, start, length);
                }
                return decode(result);
            } catch (IOException e) {
                throw new RuntimeException("Failed to apply slotted page diff", e);
            }
        }

        private record ByteRangePatch(int start, byte[] bytes) {
        }

        private int encodedSize() {
            int size = Integer.BYTES;
            for (byte[] slot : slots) {
                size += Integer.BYTES;
                if (slot != null) {
                    size += slot.length;
                }
            }
            return size;
        }

        private int maxPayload(int pageSize) {
            return pageSize - Integer.BYTES;
        }
    }
}
