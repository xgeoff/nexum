package biz.digitalindustry.db.engine;

import biz.digitalindustry.db.model.Record;
import biz.digitalindustry.db.model.RecordId;
import biz.digitalindustry.db.schema.SchemaDefinition;

import java.util.LinkedHashMap;
import java.util.Map;

public record NativeStorageState(
        Object root,
        Map<String, SchemaDefinition> schemas,
        Map<RecordId, Record> records,
        long nextId,
        long lastAppliedBatchId
) {
    public NativeStorageState {
        schemas = new LinkedHashMap<>(schemas);
        records = new LinkedHashMap<>(records);
    }

    public static NativeStorageState empty() {
        return new NativeStorageState(null, Map.of(), Map.of(), 1L, 0L);
    }
}
