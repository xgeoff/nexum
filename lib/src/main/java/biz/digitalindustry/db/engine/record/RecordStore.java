package biz.digitalindustry.db.engine.record;

import biz.digitalindustry.db.model.Record;
import biz.digitalindustry.db.model.RecordId;
import biz.digitalindustry.db.schema.EntityType;

public interface RecordStore {
    Record create(EntityType type, Record record);
    Record get(RecordId id);
    Record update(Record record);
    boolean delete(RecordId id);
    Iterable<Record> scan(EntityType type);
}
