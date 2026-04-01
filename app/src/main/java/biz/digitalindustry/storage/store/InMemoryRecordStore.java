package biz.digitalindustry.storage.store;

import biz.digitalindustry.storage.model.Record;
import biz.digitalindustry.storage.model.RecordId;
import biz.digitalindustry.storage.schema.EntityType;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class InMemoryRecordStore implements RecordStore {
    private final AtomicLong nextId = new AtomicLong(1);
    private final Map<RecordId, Record> records = new LinkedHashMap<>();

    public InMemoryRecordStore() {
    }

    public InMemoryRecordStore(long nextId, Map<RecordId, Record> initialRecords) {
        this.nextId.set(nextId);
        this.records.putAll(initialRecords);
    }

    @Override
    public synchronized Record create(EntityType type, Record record) {
        RecordId id = record.id() == null ? new RecordId(nextId.getAndIncrement()) : record.id();
        Record stored = new Record(id, type, record.fields());
        records.put(id, stored);
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
        records.put(record.id(), record);
        return record;
    }

    @Override
    public synchronized boolean delete(RecordId id) {
        return records.remove(id) != null;
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

    public synchronized long nextId() {
        return nextId.get();
    }

    public synchronized Map<RecordId, Record> snapshot() {
        return new LinkedHashMap<>(records);
    }
}
