package biz.digitalindustry.storage.engine;

import biz.digitalindustry.storage.api.RootHandle;
import biz.digitalindustry.storage.api.StorageConfig;
import biz.digitalindustry.storage.api.StorageEngine;
import biz.digitalindustry.storage.codec.RecordCodec;
import biz.digitalindustry.storage.codec.StorageSnapshotCodec;
import biz.digitalindustry.storage.log.WriteAheadLog;
import biz.digitalindustry.storage.model.BooleanValue;
import biz.digitalindustry.storage.model.DoubleValue;
import biz.digitalindustry.storage.model.Record;
import biz.digitalindustry.storage.model.RecordId;
import biz.digitalindustry.storage.model.FieldValue;
import biz.digitalindustry.storage.model.LongValue;
import biz.digitalindustry.storage.model.ReferenceValue;
import biz.digitalindustry.storage.model.StringValue;
import biz.digitalindustry.storage.page.PageFile;
import biz.digitalindustry.storage.schema.InMemorySchemaRegistry;
import biz.digitalindustry.storage.schema.SchemaRegistry;
import biz.digitalindustry.storage.schema.EntityType;
import biz.digitalindustry.storage.store.PageBackedRecordStore;
import biz.digitalindustry.storage.store.RecordStore;
import biz.digitalindustry.storage.tx.Transaction;
import biz.digitalindustry.storage.tx.TransactionMode;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.zip.CRC32;

public class NativeStorageEngine implements StorageEngine {
    private static final int BATCH_MAGIC = 0x42415443;
    private static final byte FIELD_STRING = 1;
    private static final byte FIELD_LONG = 2;
    private static final byte FIELD_DOUBLE = 3;
    private static final byte FIELD_BOOLEAN = 4;
    private static final byte FIELD_REFERENCE = 5;
    private static final byte IDENTIFIER_RECORD_ID = 1;
    private static final byte IDENTIFIER_STRING = 2;

    private StorageConfig config;
    private PageFile pageFile;
    private NativeIndexStore indexStore;
    private WriteAheadLog writeAheadLog;
    private InMemorySchemaRegistry schemaRegistry;
    private PageBackedRecordStore recordStore;
    private ExactMatchIndexManager indexManager;
    private OrderedRangeIndexManager orderedIndexManager;
    private Object root;
    private NativeStorageState txSnapshot;
    private byte[] txIndexSnapshot;
    private OrderedRangeIndexManager txOrderedIndexSnapshot;
    private boolean replayingWal;
    private long nextBatchId = 1L;
    private Long currentBatchId;
    private long lastAppliedBatchId;
    private final List<IndexWalOperation> pendingIndexOperations = new ArrayList<>();
    private final Set<String> pendingExactNamespaceOps = new LinkedHashSet<>();
    private final Set<String> pendingOrderedNamespaceOps = new LinkedHashSet<>();

    @Override
    public synchronized void open(StorageConfig config) {
        close();
        this.config = config;
        int pageSize = Math.toIntExact(config.pagePoolSize() > 0 ? config.pagePoolSize() : 8192L);
        Path dbPath = Path.of(config.path());
        pageFile = new PageFile(dbPath, pageSize);
        indexStore = new NativeIndexStore(Path.of(config.path() + ".indexes"), pageSize);
        writeAheadLog = new WriteAheadLog(Path.of(config.path() + ".wal"));
        try {
            pageFile.open();
            indexStore.open();
            writeAheadLog.open();
        } catch (IOException e) {
            close();
            throw new RuntimeException("Failed to open native storage engine at " + config.path(), e);
        }
        NativeStorageState metadata = StorageSnapshotCodec.decode(readPagePayload());
        root = metadata.root();
        schemaRegistry = new InMemorySchemaRegistry(metadata.entityTypes());
        lastAppliedBatchId = metadata.lastAppliedBatchId();
        indexManager = new ExactMatchIndexManager();
        orderedIndexManager = new OrderedRangeIndexManager();
        try {
            indexStore.loadInto(indexManager, orderedIndexManager);
        } catch (RuntimeException e) {
            indexManager.clear();
            orderedIndexManager.clear();
        }
        recordStore = new PageBackedRecordStore(
                Path.of(config.path() + ".records"),
                pageSize,
                this::resolveEntityType,
                null
        );
        recordStore.open();
        replayWriteAheadLog();
        txSnapshot = null;
        txIndexSnapshot = null;
        txOrderedIndexSnapshot = null;
        pendingIndexOperations.clear();
        pendingExactNamespaceOps.clear();
        pendingOrderedNamespaceOps.clear();
        replayingWal = false;
        currentBatchId = null;
    }

    @Override
    public synchronized boolean isOpen() {
        return pageFile != null && pageFile.isOpen()
                && indexStore != null && indexStore.isOpen()
                && writeAheadLog != null && writeAheadLog.isOpen();
    }

    @Override
    public synchronized RootHandle root() {
        ensureOpen();
        return new NativeRootHandle();
    }

    @Override
    public synchronized Transaction begin(TransactionMode mode) {
        ensureOpen();
        if (txSnapshot != null) {
            throw new IllegalStateException("Native storage engine already has an active transaction");
        }
        txSnapshot = currentState();
        txIndexSnapshot = indexManager.encode();
        txOrderedIndexSnapshot = orderedIndexManager.copy();
        currentBatchId = nextBatchId++;
        pendingIndexOperations.clear();
        pendingExactNamespaceOps.clear();
        pendingOrderedNamespaceOps.clear();
        recordStore.setAutoFlush(false);
        appendBatchEntry(WriteAheadLog.BEGIN, currentBatchId, mode.name().getBytes(StandardCharsets.UTF_8));
        return new NativeTransaction(mode);
    }

    public synchronized SchemaRegistry schemaRegistry() {
        ensureOpen();
        return schemaRegistry;
    }

    public synchronized RecordStore recordStore() {
        ensureOpen();
        return recordStore;
    }

    public synchronized PageFile pageFile() {
        ensureOpen();
        return pageFile;
    }

    public synchronized WriteAheadLog writeAheadLog() {
        ensureOpen();
        return writeAheadLog;
    }

    public synchronized StorageConfig config() {
        return config;
    }

    public synchronized ExactMatchIndexManager indexManager() {
        ensureOpen();
        return indexManager;
    }

    public synchronized OrderedRangeIndexManager orderedIndexManager() {
        ensureOpen();
        return orderedIndexManager;
    }

    public synchronized Set<Object> exactIndexFind(String namespace, String fieldName, FieldValue value) {
        ensureOpen();
        if (txSnapshot != null) {
            return indexManager.find(namespace, fieldName, value);
        }
        return indexStore.findExact((byte) 1, namespace, fieldName, value);
    }

    public synchronized Set<Object> orderedIndexRange(String namespace, String fieldName, FieldValue fromInclusive, FieldValue toInclusive) {
        ensureOpen();
        if (txSnapshot != null) {
            return orderedIndexManager.range(namespace, fieldName, fromInclusive, toInclusive);
        }
        return indexStore.findRange(namespace, fieldName, fromInclusive, toInclusive);
    }

    public synchronized <I> void ensureExactMatchNamespace(
            String namespace,
            Collection<String> indexedFields,
            EntityType entityType,
            Function<Record, I> identifierExtractor
    ) {
        ensureOpen();
        if (indexManager.ensureNamespace(namespace, indexedFields)) {
            return;
        }
        recordExactNamespaceOperation(namespace, indexedFields);
        for (Record record : recordStore.scan(entityType)) {
            I identifier = identifierExtractor.apply(record);
            if (identifier != null) {
                indexManager.add(namespace, record.fields(), identifier);
            }
        }
    }

    public synchronized <I> void ensureOrderedRangeNamespace(
            String namespace,
            Collection<String> indexedFields,
            EntityType entityType,
            Function<Record, I> identifierExtractor
    ) {
        ensureOpen();
        if (orderedIndexManager.ensureNamespace(namespace, indexedFields)) {
            return;
        }
        recordOrderedNamespaceOperation(namespace, indexedFields);
        for (Record record : recordStore.scan(entityType)) {
            I identifier = identifierExtractor.apply(record);
            if (identifier != null) {
                for (String field : indexedFields) {
                    orderedIndexManager.add(namespace, field, record.fields().get(field), identifier);
                }
            }
        }
    }

    public synchronized void exactIndexAdd(
            String namespace,
            Collection<String> indexedFields,
            Map<String, FieldValue> fieldValues,
            Object identifier
    ) {
        ensureOpen();
        if (!indexManager.ensureNamespace(namespace, indexedFields)) {
            recordExactNamespaceOperation(namespace, indexedFields);
        }
        indexManager.add(namespace, fieldValues, identifier);
        if (txSnapshot != null && !replayingWal) {
            pendingIndexOperations.add(IndexWalOperation.exactAdd(namespace, fieldValues, identifier));
        } else if (!replayingWal && indexStore != null) {
            try {
                for (String field : indexedFields) {
                    FieldValue value = fieldValues.get(field);
                    if (value != null) {
                        indexStore.applyExactAdd(namespace, field, value, identifier);
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException("Failed to update native exact index store", e);
            }
        }
    }

    public synchronized void exactIndexRemove(
            String namespace,
            Collection<String> indexedFields,
            Map<String, FieldValue> fieldValues,
            Object identifier
    ) {
        ensureOpen();
        if (!indexManager.ensureNamespace(namespace, indexedFields)) {
            recordExactNamespaceOperation(namespace, indexedFields);
        }
        indexManager.remove(namespace, fieldValues, identifier);
        if (txSnapshot != null && !replayingWal) {
            pendingIndexOperations.add(IndexWalOperation.exactRemove(namespace, fieldValues, identifier));
        } else if (!replayingWal && indexStore != null) {
            try {
                for (String field : indexedFields) {
                    FieldValue value = fieldValues.get(field);
                    if (value != null) {
                        indexStore.applyExactRemove(namespace, field, value, identifier);
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException("Failed to update native exact index store", e);
            }
        }
    }

    public synchronized void orderedIndexAdd(
            String namespace,
            Collection<String> indexedFields,
            String fieldName,
            FieldValue value,
            Object identifier
    ) {
        ensureOpen();
        if (!orderedIndexManager.ensureNamespace(namespace, indexedFields)) {
            recordOrderedNamespaceOperation(namespace, indexedFields);
        }
        orderedIndexManager.add(namespace, fieldName, value, identifier);
        if (txSnapshot != null && !replayingWal) {
            pendingIndexOperations.add(IndexWalOperation.orderedAdd(namespace, fieldName, value, identifier));
        } else if (!replayingWal && indexStore != null) {
            try {
                indexStore.applyOrderedAdd(namespace, fieldName, value, identifier);
            } catch (IOException e) {
                throw new RuntimeException("Failed to update native ordered index store", e);
            }
        }
    }

    public synchronized void orderedIndexRemove(
            String namespace,
            Collection<String> indexedFields,
            String fieldName,
            FieldValue value,
            Object identifier
    ) {
        ensureOpen();
        if (!orderedIndexManager.ensureNamespace(namespace, indexedFields)) {
            recordOrderedNamespaceOperation(namespace, indexedFields);
        }
        orderedIndexManager.remove(namespace, fieldName, value, identifier);
        if (txSnapshot != null && !replayingWal) {
            pendingIndexOperations.add(IndexWalOperation.orderedRemove(namespace, fieldName, value, identifier));
        } else if (!replayingWal && indexStore != null) {
            try {
                indexStore.applyOrderedRemove(namespace, fieldName, value, identifier);
            } catch (IOException e) {
                throw new RuntimeException("Failed to update native ordered index store", e);
            }
        }
    }

    @Override
    public synchronized void close() {
        if (isOpen()) {
            if (txSnapshot != null) {
                restoreState(txSnapshot);
                txSnapshot = null;
                if (txIndexSnapshot != null && indexManager != null) {
                    indexManager.decode(txIndexSnapshot);
                    txIndexSnapshot = null;
                }
                if (txOrderedIndexSnapshot != null) {
                    orderedIndexManager = txOrderedIndexSnapshot;
                    txOrderedIndexSnapshot = null;
                }
            }
            recordStore.setAutoFlush(true);
            recordStore.flush();
            persistState();
        }
        if (recordStore != null) {
            try {
                recordStore.close();
            } catch (IOException e) {
                throw new RuntimeException("Failed to close record store", e);
            } finally {
                recordStore = null;
            }
        }
        if (writeAheadLog != null) {
            try {
                writeAheadLog.close();
            } catch (IOException e) {
                throw new RuntimeException("Failed to close WAL", e);
            } finally {
                writeAheadLog = null;
            }
        }
        if (indexStore != null) {
            try {
                indexStore.close();
            } catch (IOException e) {
                throw new RuntimeException("Failed to close index page file", e);
            } finally {
                indexStore = null;
            }
        }
        if (pageFile != null) {
            try {
                pageFile.close();
            } catch (IOException e) {
                throw new RuntimeException("Failed to close page file", e);
            } finally {
                pageFile = null;
            }
        }
        schemaRegistry = null;
        if (indexManager != null) {
            indexManager.clear();
        }
        indexManager = null;
        if (orderedIndexManager != null) {
            orderedIndexManager.clear();
        }
        orderedIndexManager = null;
        root = null;
        txSnapshot = null;
        txIndexSnapshot = null;
        txOrderedIndexSnapshot = null;
        pendingIndexOperations.clear();
        pendingExactNamespaceOps.clear();
        pendingOrderedNamespaceOps.clear();
        config = null;
        currentBatchId = null;
        lastAppliedBatchId = 0L;
    }

    private void ensureOpen() {
        if (!isOpen()) {
            throw new IllegalStateException("Native storage engine is not open");
        }
    }

    private byte[] readPagePayload() {
        try {
            return pageFile.readPayload();
        } catch (IOException e) {
            throw new RuntimeException("Failed to read native storage payload", e);
        }
    }

    private void persistState() {
        try {
            NativeStorageState metadataState = new NativeStorageState(root, schemaRegistry.snapshot(), java.util.Map.of(), recordStore.nextId(), lastAppliedBatchId);
            pageFile.writePayload(StorageSnapshotCodec.encode(metadataState));
            persistIndexState();
            writeAheadLog.clear();
        } catch (IOException e) {
            throw new RuntimeException("Failed to persist native storage state", e);
        }
    }

    private void persistIndexState() throws IOException {
        if (indexStore != null && indexManager != null && orderedIndexManager != null) {
            indexStore.persist(indexManager, orderedIndexManager);
        }
    }

    private void recordExactNamespaceOperation(String namespace, Collection<String> indexedFields) {
        if (txSnapshot != null && !replayingWal && pendingExactNamespaceOps.add(namespace)) {
            pendingIndexOperations.add(IndexWalOperation.exactNamespace(namespace, List.copyOf(indexedFields)));
        }
    }

    private void recordOrderedNamespaceOperation(String namespace, Collection<String> indexedFields) {
        if (txSnapshot != null && !replayingWal && pendingOrderedNamespaceOps.add(namespace)) {
            pendingIndexOperations.add(IndexWalOperation.orderedNamespace(namespace, List.copyOf(indexedFields)));
        }
    }

    private NativeStorageState currentState() {
        return new NativeStorageState(
                root,
                schemaRegistry.snapshot(),
                recordStore.snapshot(),
                recordStore.nextId(),
                lastAppliedBatchId
        );
    }

    private void restoreState(NativeStorageState state) {
        root = state.root();
        schemaRegistry = new InMemorySchemaRegistry(state.entityTypes());
        lastAppliedBatchId = state.lastAppliedBatchId();
        if (recordStore != null) {
            recordStore.replaceWithSnapshot(state.records(), state.nextId());
        }
    }

    private EntityType resolveEntityType(String name) {
        EntityType entityType = schemaRegistry.entityType(name);
        return entityType != null ? entityType : new EntityType(name, List.of(), List.of());
    }

    synchronized void writeCommittedStateToWalForTest() {
        long batchId = requireCurrentBatchId();
        appendBatchEntry(WriteAheadLog.METADATA, batchId, encodeMetadataCheckpoint());
        appendIndexWalEntries(batchId);
        appendBatchEntry(WriteAheadLog.RECORD_PAGE_COUNT, batchId, encodePageCount(recordStore.currentPageCount()));
        appendBatchEntry(WriteAheadLog.RECORD_SLOT_INDEX, batchId, recordStore.encodedRecordSlotIndex());
        appendBatchEntry(WriteAheadLog.RECORD_ALLOCATOR_STATE, batchId, recordStore.encodedAllocatorState());
        for (Map.Entry<Integer, byte[]> entry : recordStore.encodedDirtyPageDiffs().entrySet()) {
            appendBatchEntry(WriteAheadLog.RECORD_PAGE_DIFF, batchId, encodePageImage(entry.getKey(), entry.getValue()));
        }
        appendBatchEntry(WriteAheadLog.COMMIT, batchId, "TEST".getBytes(StandardCharsets.UTF_8));
    }

    synchronized void writeCommittedStateWithMixedForeignDiffForTest() {
        long batchId = requireCurrentBatchId();
        appendBatchEntry(WriteAheadLog.METADATA, batchId, encodeMetadataCheckpoint());
        appendIndexWalEntries(batchId);
        appendBatchEntry(WriteAheadLog.RECORD_PAGE_COUNT, batchId, encodePageCount(recordStore.currentPageCount()));
        appendBatchEntry(WriteAheadLog.RECORD_SLOT_INDEX, batchId, recordStore.encodedRecordSlotIndex());
        appendBatchEntry(WriteAheadLog.RECORD_ALLOCATOR_STATE, batchId, recordStore.encodedAllocatorState());
        boolean insertedForeign = false;
        for (Map.Entry<Integer, byte[]> entry : recordStore.encodedDirtyPageDiffs().entrySet()) {
            if (!insertedForeign) {
                appendBatchEntry(WriteAheadLog.RECORD_PAGE_DIFF, batchId + 1000, encodePageImage(entry.getKey(), entry.getValue()));
                insertedForeign = true;
            }
            appendBatchEntry(WriteAheadLog.RECORD_PAGE_DIFF, batchId, encodePageImage(entry.getKey(), entry.getValue()));
        }
        appendBatchEntry(WriteAheadLog.COMMIT, batchId, "TEST".getBytes(StandardCharsets.UTF_8));
    }

    synchronized void persistCheckpointWithoutClearingWalForTest() {
        long batchId = requireCurrentBatchId();
        recordStore.setAutoFlush(true);
        recordStore.flush();
        lastAppliedBatchId = batchId;
        NativeStorageState metadataState = new NativeStorageState(root, schemaRegistry.snapshot(), java.util.Map.of(), recordStore.nextId(), lastAppliedBatchId);
        try {
            pageFile.writePayload(StorageSnapshotCodec.encode(metadataState));
            persistIndexState();
        } catch (IOException e) {
            throw new RuntimeException("Failed to persist checkpoint without clearing WAL", e);
        }
    }

    synchronized void flushRecordStoreWithoutEngineCheckpointForTest() {
        recordStore.setAutoFlush(true);
        recordStore.flush();
    }

    synchronized void closeWithoutCheckpointForTest() throws IOException {
        if (recordStore != null) {
            recordStore.close();
            recordStore = null;
        }
        if (writeAheadLog != null) {
            writeAheadLog.close();
            writeAheadLog = null;
        }
        if (indexStore != null) {
            indexStore.close();
            indexStore = null;
        }
        if (pageFile != null) {
            pageFile.close();
            pageFile = null;
        }
        schemaRegistry = null;
        if (indexManager != null) {
            indexManager.clear();
            indexManager = null;
        }
        if (orderedIndexManager != null) {
            orderedIndexManager.clear();
            orderedIndexManager = null;
        }
        root = null;
        txSnapshot = null;
        txIndexSnapshot = null;
        txOrderedIndexSnapshot = null;
        pendingIndexOperations.clear();
        pendingExactNamespaceOps.clear();
        pendingOrderedNamespaceOps.clear();
        config = null;
        replayingWal = false;
        currentBatchId = null;
        lastAppliedBatchId = 0L;
    }

    synchronized void corruptLastWalDiffByteForTest() throws IOException {
        if (writeAheadLog == null) {
            throw new IllegalStateException("WAL is not open");
        }
        writeAheadLog.corruptLastEntryPayloadByte(WriteAheadLog.RECORD_PAGE_DIFF);
    }

    synchronized void corruptLatestCheckpointByteForTest() throws IOException {
        if (pageFile == null) {
            throw new IllegalStateException("Page file is not open");
        }
        pageFile.corruptLatestPayloadByteForTest();
    }

    synchronized void corruptLatestRecordStoreMetadataByteForTest() throws IOException {
        if (recordStore == null) {
            throw new IllegalStateException("Record store is not open");
        }
        recordStore.corruptLatestMetadataByteForTest();
    }

    synchronized void writeIncompleteRecordStoreMetadataCheckpointForTest() throws IOException {
        if (recordStore == null) {
            throw new IllegalStateException("Record store is not open");
        }
        recordStore.writeIncompleteMetadataCheckpointForTest();
    }

    private byte[] encodeMetadataCheckpoint() {
        NativeStorageState metadataState = new NativeStorageState(root, schemaRegistry.snapshot(), java.util.Map.of(), recordStore.nextId(), lastAppliedBatchId);
        return StorageSnapshotCodec.encode(metadataState);
    }

    private byte[] encodeDelete(RecordId recordId) {
        return ByteBuffer.allocate(Long.BYTES).putLong(recordId.value()).array();
    }

    private RecordId decodeDelete(byte[] payload) {
        return new RecordId(ByteBuffer.wrap(payload).getLong());
    }

    private void appendBatchEntry(byte type, long batchId, byte[] payload) {
        writeAheadLog.append(type, encodeBatchPayload(batchId, payload));
    }

    private void appendIndexWalEntries(long batchId) {
        for (IndexWalOperation operation : pendingIndexOperations) {
            appendBatchEntry(operation.entryType(), batchId, encodeIndexWalPayload(operation));
        }
    }

    private byte[] encodeBatchPayload(long batchId, byte[] payload) {
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES + Long.BYTES + payload.length);
        buffer.putInt(BATCH_MAGIC);
        buffer.putLong(batchId);
        buffer.put(payload);
        return buffer.array();
    }

    private BatchPayload decodeBatchPayload(byte[] payload) {
        if (payload.length >= Integer.BYTES + Long.BYTES) {
            ByteBuffer buffer = ByteBuffer.wrap(payload);
            int magic = buffer.getInt();
            if (magic == BATCH_MAGIC) {
                long batchId = buffer.getLong();
                byte[] inner = new byte[buffer.remaining()];
                buffer.get(inner);
                return new BatchPayload(batchId, inner);
            }
        }
        return new BatchPayload(null, payload);
    }

    private boolean matchesActiveBatch(Long activeBatchId, Long entryBatchId) {
        if (activeBatchId == null) {
            return entryBatchId == null;
        }
        return activeBatchId.equals(entryBatchId);
    }

    private long requireCurrentBatchId() {
        if (currentBatchId == null) {
            throw new IllegalStateException("No active native WAL batch");
        }
        return currentBatchId;
    }

    private void replayWriteAheadLog() {
        List<WriteAheadLog.Entry> entries = writeAheadLog.readEntries();
        if (entries.isEmpty()) {
            return;
        }

        List<WriteAheadLog.Entry> pendingOperations = new ArrayList<>();
        NativeStorageState committedMetadata = null;
        List<IndexWalOperation> committedIndexOperations = new ArrayList<>();
        Integer committedPageCount = null;
        byte[] committedRecordSlotIndex = null;
        byte[] committedAllocatorState = null;
        Map<Integer, byte[]> committedPageDiffs = new LinkedHashMap<>();
        boolean replayed = false;
        boolean corruptedBatch = false;
        Long activeBatchId = null;

        for (WriteAheadLog.Entry entry : entries) {
            BatchPayload batchPayload = decodeBatchPayload(entry.payload());
            switch (entry.type()) {
                case WriteAheadLog.BEGIN -> {
                    activeBatchId = batchPayload.batchId();
                    pendingOperations.clear();
                    committedMetadata = null;
                    committedIndexOperations = new ArrayList<>();
                    committedPageCount = null;
                    committedRecordSlotIndex = null;
                    committedAllocatorState = null;
                    committedPageDiffs = new LinkedHashMap<>();
                    corruptedBatch = false;
                }
                case WriteAheadLog.UPSERT, WriteAheadLog.DELETE -> {
                    if (matchesActiveBatch(activeBatchId, batchPayload.batchId())) {
                        pendingOperations.add(new WriteAheadLog.Entry(entry.type(), batchPayload.payload()));
                    }
                }
                case WriteAheadLog.METADATA -> {
                    if (matchesActiveBatch(activeBatchId, batchPayload.batchId())) {
                        committedMetadata = StorageSnapshotCodec.decode(batchPayload.payload());
                    }
                }
                case WriteAheadLog.EXACT_INDEX_NAMESPACE, WriteAheadLog.EXACT_INDEX_ADD, WriteAheadLog.EXACT_INDEX_REMOVE,
                        WriteAheadLog.ORDERED_INDEX_NAMESPACE, WriteAheadLog.ORDERED_INDEX_ADD, WriteAheadLog.ORDERED_INDEX_REMOVE -> {
                    if (matchesActiveBatch(activeBatchId, batchPayload.batchId())) {
                        committedIndexOperations.add(decodeIndexWalOperation(entry.type(), batchPayload.payload()));
                    }
                }
                case WriteAheadLog.RECORD_PAGE_COUNT -> {
                    if (matchesActiveBatch(activeBatchId, batchPayload.batchId())) {
                        committedPageCount = ByteBuffer.wrap(batchPayload.payload()).getInt();
                    }
                }
                case WriteAheadLog.RECORD_SLOT_INDEX -> {
                    if (matchesActiveBatch(activeBatchId, batchPayload.batchId())) {
                        committedRecordSlotIndex = batchPayload.payload();
                    }
                }
                case WriteAheadLog.RECORD_ALLOCATOR_STATE -> {
                    if (matchesActiveBatch(activeBatchId, batchPayload.batchId())) {
                        committedAllocatorState = batchPayload.payload();
                    }
                }
                case WriteAheadLog.RECORD_PAGE_DIFF -> {
                    if (matchesActiveBatch(activeBatchId, batchPayload.batchId())) {
                        try {
                            committedPageDiffs.put(decodePageNo(batchPayload.payload()), decodePagePayload(batchPayload.payload()));
                        } catch (RuntimeException e) {
                            corruptedBatch = true;
                        }
                    }
                }
                case WriteAheadLog.COMMIT -> {
                    if (matchesActiveBatch(activeBatchId, batchPayload.batchId()) && committedMetadata != null && !corruptedBatch
                            && (activeBatchId == null || activeBatchId > lastAppliedBatchId)) {
                        if (committedPageCount != null && committedRecordSlotIndex != null && committedAllocatorState != null) {
                            applyRecoveredPageDiffTransaction(committedMetadata, committedPageCount, committedRecordSlotIndex, committedAllocatorState, committedPageDiffs);
                        } else {
                            applyRecoveredLogicalTransaction(committedMetadata, pendingOperations);
                        }
                        applyRecoveredIndexState(committedIndexOperations);
                        if (activeBatchId != null) {
                            lastAppliedBatchId = activeBatchId;
                        }
                        replayed = true;
                    }
                    pendingOperations.clear();
                    committedMetadata = null;
                    committedIndexOperations = new ArrayList<>();
                    committedPageCount = null;
                    committedRecordSlotIndex = null;
                    committedAllocatorState = null;
                    committedPageDiffs = new LinkedHashMap<>();
                    corruptedBatch = false;
                    activeBatchId = null;
                }
                case WriteAheadLog.ROLLBACK -> {
                    if (matchesActiveBatch(activeBatchId, batchPayload.batchId())) {
                        pendingOperations.clear();
                        committedMetadata = null;
                        committedIndexOperations = new ArrayList<>();
                        committedPageCount = null;
                        committedRecordSlotIndex = null;
                        committedAllocatorState = null;
                        committedPageDiffs = new LinkedHashMap<>();
                        corruptedBatch = false;
                        activeBatchId = null;
                    }
                }
                default -> {
                }
            }
        }

        if (replayed) {
            persistState();
        }
    }

    private void applyRecoveredIndexState(List<IndexWalOperation> committedIndexOperations) {
        for (IndexWalOperation operation : committedIndexOperations) {
            applyIndexWalOperation(operation);
        }
    }

    private void applyRecoveredLogicalTransaction(NativeStorageState metadataState, List<WriteAheadLog.Entry> operations) {
        replayingWal = true;
        try {
            root = metadataState.root();
            schemaRegistry = new InMemorySchemaRegistry(metadataState.entityTypes());
            recordStore.setAutoFlush(false);
            for (WriteAheadLog.Entry operation : operations) {
                if (operation.type() == WriteAheadLog.UPSERT) {
                    recordStore.recoverUpsert(RecordCodec.decode(operation.payload(), this::resolveEntityType));
                } else if (operation.type() == WriteAheadLog.DELETE) {
                    recordStore.recoverDelete(decodeDelete(operation.payload()));
                }
            }
            recordStore.setNextId(metadataState.nextId());
            recordStore.setAutoFlush(true);
            recordStore.flush();
        } finally {
            if (recordStore != null) {
                recordStore.setAutoFlush(true);
            }
            replayingWal = false;
        }
    }

    private void applyRecoveredPageDiffTransaction(
            NativeStorageState metadataState,
            int pageCount,
            byte[] recordSlotIndex,
            byte[] allocatorState,
            Map<Integer, byte[]> pageDiffs
    ) {
        replayingWal = true;
        try {
            root = metadataState.root();
            schemaRegistry = new InMemorySchemaRegistry(metadataState.entityTypes());
            recordStore.applyRecoveredPageDiffState(pageCount, recordSlotIndex, allocatorState, pageDiffs);
        } finally {
            replayingWal = false;
        }
    }

    private byte[] encodePageCount(int pageCount) {
        return ByteBuffer.allocate(Integer.BYTES).putInt(pageCount).array();
    }

    private void applyIndexWalOperation(IndexWalOperation operation) {
        switch (operation.kind()) {
            case EXACT_NAMESPACE -> indexManager.ensureNamespace(operation.namespace(), operation.indexedFields());
            case EXACT_ADD -> {
                indexManager.ensureNamespace(operation.namespace(), operation.fieldValues().keySet());
                indexManager.add(operation.namespace(), operation.fieldValues(), operation.identifier());
            }
            case EXACT_REMOVE -> {
                indexManager.ensureNamespace(operation.namespace(), operation.fieldValues().keySet());
                indexManager.remove(operation.namespace(), operation.fieldValues(), operation.identifier());
            }
            case ORDERED_NAMESPACE -> orderedIndexManager.ensureNamespace(operation.namespace(), operation.indexedFields());
            case ORDERED_ADD -> {
                orderedIndexManager.ensureNamespace(operation.namespace(), operation.indexedFields());
                orderedIndexManager.add(operation.namespace(), operation.fieldName(), operation.fieldValue(), operation.identifier());
            }
            case ORDERED_REMOVE -> {
                orderedIndexManager.ensureNamespace(operation.namespace(), operation.indexedFields());
                orderedIndexManager.remove(operation.namespace(), operation.fieldName(), operation.fieldValue(), operation.identifier());
            }
        }
    }

    private IndexWalOperation decodeIndexWalOperation(byte entryType, byte[] payload) {
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(payload));
            String namespace = in.readUTF();
            return switch (entryType) {
                case WriteAheadLog.EXACT_INDEX_NAMESPACE -> IndexWalOperation.exactNamespace(namespace, readStringList(in));
                case WriteAheadLog.EXACT_INDEX_ADD -> IndexWalOperation.exactAdd(namespace, readFieldValueMap(in), readIdentifier(in));
                case WriteAheadLog.EXACT_INDEX_REMOVE -> IndexWalOperation.exactRemove(namespace, readFieldValueMap(in), readIdentifier(in));
                case WriteAheadLog.ORDERED_INDEX_NAMESPACE -> IndexWalOperation.orderedNamespace(namespace, readStringList(in));
                case WriteAheadLog.ORDERED_INDEX_ADD -> IndexWalOperation.orderedAdd(namespace, in.readUTF(), readFieldValue(in), readIdentifier(in), readStringList(in));
                case WriteAheadLog.ORDERED_INDEX_REMOVE -> IndexWalOperation.orderedRemove(namespace, in.readUTF(), readFieldValue(in), readIdentifier(in), readStringList(in));
                default -> throw new IOException("Unsupported index WAL entry type " + entryType);
            };
        } catch (IOException e) {
            throw new RuntimeException("Failed to decode index WAL operation", e);
        }
    }

    private byte[] encodeIndexWalPayload(IndexWalOperation operation) {
        try {
            ByteArrayOutputStream buffer = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(buffer);
            out.writeUTF(operation.namespace());
            switch (operation.kind()) {
                case EXACT_NAMESPACE, ORDERED_NAMESPACE -> writeStringList(out, operation.indexedFields());
                case EXACT_ADD, EXACT_REMOVE -> {
                    writeFieldValueMap(out, operation.fieldValues());
                    writeIdentifier(out, operation.identifier());
                }
                case ORDERED_ADD, ORDERED_REMOVE -> {
                    out.writeUTF(operation.fieldName());
                    writeFieldValue(out, operation.fieldValue());
                    writeIdentifier(out, operation.identifier());
                    writeStringList(out, operation.indexedFields());
                }
            }
            out.flush();
            return buffer.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Failed to encode index WAL operation", e);
        }
    }

    private void writeStringList(DataOutputStream out, List<String> values) throws IOException {
        out.writeInt(values.size());
        for (String value : values) {
            out.writeUTF(value);
        }
    }

    private List<String> readStringList(DataInputStream in) throws IOException {
        int size = in.readInt();
        List<String> values = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            values.add(in.readUTF());
        }
        return values;
    }

    private void writeFieldValueMap(DataOutputStream out, Map<String, FieldValue> fieldValues) throws IOException {
        out.writeInt(fieldValues.size());
        for (Map.Entry<String, FieldValue> entry : fieldValues.entrySet()) {
            out.writeUTF(entry.getKey());
            writeFieldValue(out, entry.getValue());
        }
    }

    private Map<String, FieldValue> readFieldValueMap(DataInputStream in) throws IOException {
        int size = in.readInt();
        Map<String, FieldValue> values = new LinkedHashMap<>();
        for (int i = 0; i < size; i++) {
            values.put(in.readUTF(), readFieldValue(in));
        }
        return values;
    }

    private void writeFieldValue(DataOutputStream out, FieldValue value) throws IOException {
        if (value instanceof StringValue stringValue) {
            out.writeByte(FIELD_STRING);
            out.writeUTF(stringValue.value());
            return;
        }
        if (value instanceof LongValue longValue) {
            out.writeByte(FIELD_LONG);
            out.writeLong(longValue.value());
            return;
        }
        if (value instanceof DoubleValue doubleValue) {
            out.writeByte(FIELD_DOUBLE);
            out.writeDouble(doubleValue.value());
            return;
        }
        if (value instanceof BooleanValue booleanValue) {
            out.writeByte(FIELD_BOOLEAN);
            out.writeBoolean(booleanValue.value());
            return;
        }
        if (value instanceof ReferenceValue referenceValue) {
            out.writeByte(FIELD_REFERENCE);
            out.writeLong(referenceValue.recordId().value());
            return;
        }
        throw new IOException("Unsupported indexed field value: " + value);
    }

    private FieldValue readFieldValue(DataInputStream in) throws IOException {
        byte type = in.readByte();
        return switch (type) {
            case FIELD_STRING -> new StringValue(in.readUTF());
            case FIELD_LONG -> new LongValue(in.readLong());
            case FIELD_DOUBLE -> new DoubleValue(in.readDouble());
            case FIELD_BOOLEAN -> new BooleanValue(in.readBoolean());
            case FIELD_REFERENCE -> new ReferenceValue(new RecordId(in.readLong()));
            default -> throw new IOException("Unsupported indexed field value type " + type);
        };
    }

    private void writeIdentifier(DataOutputStream out, Object identifier) throws IOException {
        if (identifier instanceof RecordId recordId) {
            out.writeByte(IDENTIFIER_RECORD_ID);
            out.writeLong(recordId.value());
            return;
        }
        if (identifier instanceof String stringValue) {
            out.writeByte(IDENTIFIER_STRING);
            out.writeUTF(stringValue);
            return;
        }
        throw new IOException("Unsupported index identifier: " + identifier);
    }

    private Object readIdentifier(DataInputStream in) throws IOException {
        byte type = in.readByte();
        return switch (type) {
            case IDENTIFIER_RECORD_ID -> new RecordId(in.readLong());
            case IDENTIFIER_STRING -> in.readUTF();
            default -> throw new IOException("Unsupported index identifier type " + type);
        };
    }

    private byte[] encodePageImage(int pageNo, byte[] payload) {
        CRC32 crc32 = new CRC32();
        crc32.update(payload);
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES * 3 + payload.length);
        buffer.putInt(pageNo);
        buffer.putInt(payload.length);
        buffer.putInt((int) crc32.getValue());
        buffer.put(payload);
        return buffer.array();
    }

    private int decodePageNo(byte[] payload) {
        return ByteBuffer.wrap(payload).getInt();
    }

    private byte[] decodePagePayload(byte[] payload) {
        ByteBuffer buffer = ByteBuffer.wrap(payload);
        buffer.getInt();
        int length = buffer.getInt();
        int remaining = buffer.remaining();
        byte[] pagePayload;
        if (remaining == length) {
            pagePayload = new byte[length];
            buffer.get(pagePayload);
        } else if (remaining == length + Integer.BYTES) {
            int expectedChecksum = buffer.getInt();
            pagePayload = new byte[length];
            buffer.get(pagePayload);
            CRC32 crc32 = new CRC32();
            crc32.update(pagePayload);
            if ((int) crc32.getValue() != expectedChecksum) {
                throw new IllegalStateException("Corrupt WAL page payload checksum");
            }
        } else {
            throw new IllegalStateException("Invalid WAL page payload encoding");
        }
        return pagePayload;
    }

    private record BatchPayload(Long batchId, byte[] payload) {
    }

    private record IndexWalOperation(
            IndexWalKind kind,
            String namespace,
            List<String> indexedFields,
            Map<String, FieldValue> fieldValues,
            String fieldName,
            FieldValue fieldValue,
            Object identifier
    ) {
        static IndexWalOperation exactNamespace(String namespace, List<String> indexedFields) {
            return new IndexWalOperation(IndexWalKind.EXACT_NAMESPACE, namespace, indexedFields, Map.of(), null, null, null);
        }

        static IndexWalOperation exactAdd(String namespace, Map<String, FieldValue> fieldValues, Object identifier) {
            return new IndexWalOperation(IndexWalKind.EXACT_ADD, namespace, List.copyOf(fieldValues.keySet()), new LinkedHashMap<>(fieldValues), null, null, identifier);
        }

        static IndexWalOperation exactRemove(String namespace, Map<String, FieldValue> fieldValues, Object identifier) {
            return new IndexWalOperation(IndexWalKind.EXACT_REMOVE, namespace, List.copyOf(fieldValues.keySet()), new LinkedHashMap<>(fieldValues), null, null, identifier);
        }

        static IndexWalOperation orderedNamespace(String namespace, List<String> indexedFields) {
            return new IndexWalOperation(IndexWalKind.ORDERED_NAMESPACE, namespace, indexedFields, Map.of(), null, null, null);
        }

        static IndexWalOperation orderedAdd(String namespace, String fieldName, FieldValue fieldValue, Object identifier) {
            return orderedAdd(namespace, fieldName, fieldValue, identifier, List.of(fieldName));
        }

        static IndexWalOperation orderedAdd(String namespace, String fieldName, FieldValue fieldValue, Object identifier, List<String> indexedFields) {
            return new IndexWalOperation(IndexWalKind.ORDERED_ADD, namespace, indexedFields, Map.of(), fieldName, fieldValue, identifier);
        }

        static IndexWalOperation orderedRemove(String namespace, String fieldName, FieldValue fieldValue, Object identifier) {
            return orderedRemove(namespace, fieldName, fieldValue, identifier, List.of(fieldName));
        }

        static IndexWalOperation orderedRemove(String namespace, String fieldName, FieldValue fieldValue, Object identifier, List<String> indexedFields) {
            return new IndexWalOperation(IndexWalKind.ORDERED_REMOVE, namespace, indexedFields, Map.of(), fieldName, fieldValue, identifier);
        }

        byte entryType() {
            return switch (kind) {
                case EXACT_NAMESPACE -> WriteAheadLog.EXACT_INDEX_NAMESPACE;
                case EXACT_ADD -> WriteAheadLog.EXACT_INDEX_ADD;
                case EXACT_REMOVE -> WriteAheadLog.EXACT_INDEX_REMOVE;
                case ORDERED_NAMESPACE -> WriteAheadLog.ORDERED_INDEX_NAMESPACE;
                case ORDERED_ADD -> WriteAheadLog.ORDERED_INDEX_ADD;
                case ORDERED_REMOVE -> WriteAheadLog.ORDERED_INDEX_REMOVE;
            };
        }
    }

    private enum IndexWalKind {
        EXACT_NAMESPACE,
        EXACT_ADD,
        EXACT_REMOVE,
        ORDERED_NAMESPACE,
        ORDERED_ADD,
        ORDERED_REMOVE
    }

    private final class NativeRootHandle implements RootHandle {
        @Override
        public Object get() {
            return root;
        }

        @Override
        public <T> T get(Class<T> type) {
            return type.isInstance(root) ? type.cast(root) : null;
        }

        @Override
        public void set(Object root) {
            NativeStorageEngine.this.root = root;
        }
    }

    private final class NativeTransaction implements Transaction {
        private final TransactionMode mode;
        private boolean active = true;

        private NativeTransaction(TransactionMode mode) {
            this.mode = mode;
        }

        @Override
        public void commit() {
            if (active) {
                long batchId = requireCurrentBatchId();
                appendBatchEntry(WriteAheadLog.METADATA, batchId, encodeMetadataCheckpoint());
                appendIndexWalEntries(batchId);
                appendBatchEntry(WriteAheadLog.RECORD_PAGE_COUNT, batchId, encodePageCount(recordStore.currentPageCount()));
                appendBatchEntry(WriteAheadLog.RECORD_SLOT_INDEX, batchId, recordStore.encodedRecordSlotIndex());
                appendBatchEntry(WriteAheadLog.RECORD_ALLOCATOR_STATE, batchId, recordStore.encodedAllocatorState());
                for (Map.Entry<Integer, byte[]> entry : recordStore.encodedDirtyPageDiffs().entrySet()) {
                    appendBatchEntry(WriteAheadLog.RECORD_PAGE_DIFF, batchId, encodePageImage(entry.getKey(), entry.getValue()));
                }
                appendBatchEntry(WriteAheadLog.COMMIT, batchId, mode.name().getBytes(StandardCharsets.UTF_8));
                recordStore.setAutoFlush(true);
                recordStore.flush();
                lastAppliedBatchId = batchId;
                persistState();
                txSnapshot = null;
                txIndexSnapshot = null;
                txOrderedIndexSnapshot = null;
                pendingIndexOperations.clear();
                pendingExactNamespaceOps.clear();
                pendingOrderedNamespaceOps.clear();
                currentBatchId = null;
                active = false;
            }
        }

        @Override
        public void rollback() {
            if (active) {
                appendBatchEntry(WriteAheadLog.ROLLBACK, requireCurrentBatchId(), mode.name().getBytes(StandardCharsets.UTF_8));
                if (txSnapshot != null) {
                    restoreState(txSnapshot);
                    txSnapshot = null;
                }
                if (txIndexSnapshot != null && indexManager != null) {
                    indexManager.decode(txIndexSnapshot);
                    txIndexSnapshot = null;
                }
                if (txOrderedIndexSnapshot != null) {
                    orderedIndexManager = txOrderedIndexSnapshot;
                    txOrderedIndexSnapshot = null;
                }
                pendingIndexOperations.clear();
                pendingExactNamespaceOps.clear();
                pendingOrderedNamespaceOps.clear();
                recordStore.setAutoFlush(true);
                recordStore.flush();
                currentBatchId = null;
                active = false;
            }
        }

        @Override
        public void close() {
            rollback();
        }
    }
}
