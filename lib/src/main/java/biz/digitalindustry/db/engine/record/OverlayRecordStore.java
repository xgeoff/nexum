package biz.digitalindustry.db.engine.record;

import biz.digitalindustry.db.engine.log.WriteAheadLog;
import biz.digitalindustry.db.model.Record;
import biz.digitalindustry.db.model.RecordId;
import biz.digitalindustry.db.schema.EntityType;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class OverlayRecordStore implements RecordStore {
    private final RecordStore committedStore;
    private final Map<RecordId, Record> upserts = new LinkedHashMap<>();
    private final Set<RecordId> deletes = new LinkedHashSet<>();
    private final List<RecordMutation> mutations = new ArrayList<>();
    private long nextId;

    public OverlayRecordStore(RecordStore committedStore, long nextId) {
        this.committedStore = committedStore;
        this.nextId = nextId;
    }

    @Override
    public synchronized Record create(EntityType type, Record record) {
        RecordId id = record.id() == null ? new RecordId(nextId++) : record.id();
        nextId = Math.max(nextId, id.value() + 1);
        Record stored = new Record(id, type, record.fields());
        upserts.put(id, stored);
        deletes.remove(id);
        mutations.add(RecordMutation.upsert(stored));
        return stored;
    }

    @Override
    public synchronized Record get(RecordId id) {
        if (deletes.contains(id)) {
            return null;
        }
        Record overlay = upserts.get(id);
        return overlay != null ? overlay : committedStore.get(id);
    }

    @Override
    public synchronized Record update(Record record) {
        if (record.id() == null) {
            throw new IllegalArgumentException("Unknown record id: null");
        }
        Record existing = get(record.id());
        if (existing == null) {
            throw new IllegalArgumentException("Unknown record id: " + record.id().value());
        }
        upserts.put(record.id(), record);
        deletes.remove(record.id());
        mutations.add(RecordMutation.upsert(record));
        return record;
    }

    @Override
    public synchronized boolean delete(RecordId id) {
        Record existing = get(id);
        if (existing == null) {
            return false;
        }
        upserts.remove(id);
        deletes.add(id);
        mutations.add(RecordMutation.delete(id));
        return true;
    }

    @Override
    public synchronized Iterable<Record> scan(EntityType type) {
        List<Record> matches = new ArrayList<>();
        Set<RecordId> included = new LinkedHashSet<>();
        for (Record record : committedStore.scan(type)) {
            if (deletes.contains(record.id())) {
                continue;
            }
            Record overlay = upserts.get(record.id());
            Record resolved = overlay != null ? overlay : record;
            if (resolved.type().name().equals(type.name())) {
                matches.add(resolved);
                included.add(resolved.id());
            }
        }
        for (Record record : upserts.values()) {
            if (!included.contains(record.id()) && record.type().name().equals(type.name())) {
                matches.add(record);
            }
        }
        return matches;
    }

    public synchronized long nextId() {
        return nextId;
    }

    public synchronized List<RecordMutation> mutations() {
        return List.copyOf(mutations);
    }

    public synchronized Map<RecordId, Record> upserts() {
        return new LinkedHashMap<>(upserts);
    }

    public synchronized Set<RecordId> deletes() {
        return new LinkedHashSet<>(deletes);
    }

    public record RecordMutation(byte operationType, Record record, RecordId recordId) {
        public static RecordMutation upsert(Record record) {
            return new RecordMutation(WriteAheadLog.UPSERT, record, record.id());
        }

        public static RecordMutation delete(RecordId recordId) {
            return new RecordMutation(WriteAheadLog.DELETE, null, recordId);
        }
    }
}
