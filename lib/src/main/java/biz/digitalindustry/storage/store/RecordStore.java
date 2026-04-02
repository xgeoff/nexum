package biz.digitalindustry.storage.store;

import biz.digitalindustry.storage.model.Record;
import biz.digitalindustry.storage.model.RecordId;
import biz.digitalindustry.storage.schema.EntityType;

public interface RecordStore {
    Record create(EntityType type, Record record);
    Record get(RecordId id);
    Record update(Record record);
    boolean delete(RecordId id);
    Iterable<Record> scan(EntityType type);
}
