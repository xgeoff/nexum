package biz.digitalindustry.storage.engine;

import biz.digitalindustry.storage.model.Record;
import biz.digitalindustry.storage.model.RecordId;
import biz.digitalindustry.storage.schema.EntityType;

import java.util.LinkedHashMap;
import java.util.Map;

public record NativeStorageState(
        Object root,
        Map<String, EntityType> entityTypes,
        Map<RecordId, Record> records,
        long nextId,
        long lastAppliedBatchId
) {
    public NativeStorageState {
        entityTypes = new LinkedHashMap<>(entityTypes);
        records = new LinkedHashMap<>(records);
    }

    public static NativeStorageState empty() {
        return new NativeStorageState(null, Map.of(), Map.of(), 1L, 0L);
    }
}
