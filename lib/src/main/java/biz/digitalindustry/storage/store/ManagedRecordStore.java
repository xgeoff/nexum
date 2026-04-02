package biz.digitalindustry.storage.store;

import biz.digitalindustry.storage.model.Record;
import biz.digitalindustry.storage.model.RecordId;

import java.io.Closeable;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

public interface ManagedRecordStore extends RecordStore, Closeable {
    void open();
    long nextId();
    Map<RecordId, Record> snapshot();
    void recoverUpsert(Record record);
    void recoverDelete(RecordId id);
    void setNextId(long restoredNextId);
    void setAutoFlush(boolean autoFlush);
    void flush();
    int currentPageCount();
    byte[] encodedRecordSlotIndex();
    byte[] encodedAllocatorState();
    Map<Integer, byte[]> encodedDirtyPages();
    Map<Integer, byte[]> encodedDirtyPageDiffs();
    void applyRecoveredPageDiffState(int pageCount, byte[] recordSlotIndexPayload, byte[] allocatorStatePayload, Map<Integer, byte[]> pageDiffPayloads);
    void corruptLatestMetadataByteForTest() throws IOException;
    void writeIncompleteMetadataCheckpointForTest() throws IOException;
}
