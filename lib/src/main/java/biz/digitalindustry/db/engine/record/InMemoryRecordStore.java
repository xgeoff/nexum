package biz.digitalindustry.db.engine.record;

import biz.digitalindustry.db.engine.codec.RecordCodec;
import biz.digitalindustry.db.engine.log.WriteAheadLog;
import biz.digitalindustry.db.model.Record;
import biz.digitalindustry.db.model.RecordId;
import biz.digitalindustry.db.schema.EntityType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class InMemoryRecordStore implements ManagedRecordStore {
    private final AtomicLong nextId = new AtomicLong(1L);
    private final Map<RecordId, Record> records = new LinkedHashMap<>();
    private final PageBackedRecordStore.MutationListener mutationListener;

    public InMemoryRecordStore(PageBackedRecordStore.MutationListener mutationListener) {
        this.mutationListener = mutationListener;
    }

    @Override
    public synchronized void open() {
    }

    @Override
    public synchronized Record create(EntityType type, Record record) {
        RecordId id = record.id() == null ? new RecordId(nextId.getAndIncrement()) : record.id();
        Record stored = new Record(id, type, record.fields());
        nextId.set(Math.max(nextId.get(), id.value() + 1));
        records.put(id, stored);
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
        Record previous = records.put(record.id(), record);
        notifyMutation(WriteAheadLog.UPSERT, previous, record, record.id());
        return record;
    }

    @Override
    public synchronized boolean delete(RecordId id) {
        Record previous = records.remove(id);
        if (previous == null) {
            return false;
        }
        notifyMutation(WriteAheadLog.DELETE, previous, null, id);
        return true;
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

    @Override
    public synchronized long nextId() {
        return nextId.get();
    }

    @Override
    public synchronized Map<RecordId, Record> snapshot() {
        return new LinkedHashMap<>(records);
    }

    @Override
    public synchronized void recoverUpsert(Record record) {
        nextId.set(Math.max(nextId.get(), record.id().value() + 1));
        records.put(record.id(), record);
    }

    @Override
    public synchronized void recoverDelete(RecordId id) {
        records.remove(id);
    }

    @Override
    public synchronized void setNextId(long restoredNextId) {
        nextId.set(restoredNextId);
    }

    @Override
    public synchronized void setAutoFlush(boolean autoFlush) {
    }

    @Override
    public synchronized void flush() {
    }

    @Override
    public synchronized int currentPageCount() {
        return records.isEmpty() ? 0 : 1;
    }

    @Override
    public synchronized byte[] encodedRecordSlotIndex() {
        return new byte[0];
    }

    @Override
    public synchronized byte[] encodedAllocatorState() {
        return new byte[0];
    }

    @Override
    public synchronized Map<Integer, byte[]> encodedDirtyPages() {
        return Map.of();
    }

    @Override
    public synchronized Map<Integer, byte[]> encodedDirtyPageDiffs() {
        return Map.of();
    }

    @Override
    public synchronized void applyRecoveredPageDiffState(
            int pageCount,
            byte[] recordSlotIndexPayload,
            byte[] allocatorStatePayload,
            Map<Integer, byte[]> pageDiffPayloads
    ) {
    }

    @Override
    public synchronized void corruptLatestMetadataByteForTest() throws IOException {
        throw new IOException("In-memory record store does not persist metadata");
    }

    @Override
    public synchronized void writeIncompleteMetadataCheckpointForTest() throws IOException {
        throw new IOException("In-memory record store does not persist metadata");
    }

    @Override
    public synchronized void close() {
        records.clear();
    }

    private void notifyMutation(byte operationType, Record beforeRecord, Record afterRecord, RecordId recordId) {
        if (mutationListener != null) {
            mutationListener.onMutation(operationType, beforeRecord, afterRecord, recordId);
        }
    }
}
