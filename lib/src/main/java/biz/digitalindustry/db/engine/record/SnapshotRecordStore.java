package biz.digitalindustry.db.engine.record;

import biz.digitalindustry.db.model.Record;
import biz.digitalindustry.db.model.RecordId;
import biz.digitalindustry.db.schema.EntityType;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class SnapshotRecordStore implements RecordStore {
    private final Map<RecordId, Record> records;

    public SnapshotRecordStore(Map<RecordId, Record> records) {
        this.records = new LinkedHashMap<>(records);
    }

    @Override
    public Record create(EntityType type, Record record) {
        throw new UnsupportedOperationException("Snapshot record store is read-only");
    }

    @Override
    public Record get(RecordId id) {
        return records.get(id);
    }

    @Override
    public Record update(Record record) {
        throw new UnsupportedOperationException("Snapshot record store is read-only");
    }

    @Override
    public boolean delete(RecordId id) {
        throw new UnsupportedOperationException("Snapshot record store is read-only");
    }

    @Override
    public Iterable<Record> scan(EntityType type) {
        List<Record> matches = new ArrayList<>();
        for (Record record : records.values()) {
            if (record.type().name().equals(type.name())) {
                matches.add(record);
            }
        }
        return matches;
    }
}
