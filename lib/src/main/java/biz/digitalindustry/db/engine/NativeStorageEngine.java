package biz.digitalindustry.db.engine;

import biz.digitalindustry.db.engine.api.RootHandle;
import biz.digitalindustry.db.engine.api.StorageConfig;
import biz.digitalindustry.db.engine.api.StorageEngine;
import biz.digitalindustry.db.engine.codec.RecordCodec;
import biz.digitalindustry.db.engine.codec.StorageSnapshotCodec;
import biz.digitalindustry.db.index.OrderedRangeIndex;
import biz.digitalindustry.db.index.VectorMatch;
import biz.digitalindustry.db.engine.log.WriteAheadLog;
import biz.digitalindustry.db.model.BooleanValue;
import biz.digitalindustry.db.model.DoubleValue;
import biz.digitalindustry.db.model.Record;
import biz.digitalindustry.db.model.RecordId;
import biz.digitalindustry.db.model.FieldValue;
import biz.digitalindustry.db.model.LongValue;
import biz.digitalindustry.db.model.ReferenceValue;
import biz.digitalindustry.db.model.StringValue;
import biz.digitalindustry.db.model.Vector;
import biz.digitalindustry.db.model.VectorValue;
import biz.digitalindustry.db.engine.page.PageFile;
import biz.digitalindustry.db.schema.InMemorySchemaRegistry;
import biz.digitalindustry.db.schema.SchemaRegistry;
import biz.digitalindustry.db.schema.EntityType;
import biz.digitalindustry.db.schema.SchemaDefinition;
import biz.digitalindustry.db.engine.record.InMemoryRecordStore;
import biz.digitalindustry.db.engine.record.ManagedRecordStore;
import biz.digitalindustry.db.engine.record.OverlayRecordStore;
import biz.digitalindustry.db.engine.record.PageBackedRecordStore;
import biz.digitalindustry.db.engine.record.RecordStore;
import biz.digitalindustry.db.engine.record.SnapshotRecordStore;
import biz.digitalindustry.db.engine.tx.Transaction;
import biz.digitalindustry.db.engine.tx.TransactionMode;
import biz.digitalindustry.db.vector.VectorIndexDefinition;

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
    private static final byte FIELD_VECTOR = 6;
    private static final byte IDENTIFIER_RECORD_ID = 1;
    private static final byte IDENTIFIER_STRING = 2;

    private StorageConfig config;
    private PageFile pageFile;
    private NativeIndexStore indexStore;
    private WriteAheadLog writeAheadLog;
    private InMemorySchemaRegistry schemaRegistry;
    private ManagedRecordStore recordStore;
    private ExactMatchIndexManager indexManager;
    private OrderedRangeIndexManager orderedIndexManager;
    private VectorIndexManager vectorIndexManager;
    private Object root;
    private Object txRootSnapshot;
    private Map<String, SchemaDefinition> txSchemaSnapshot;
    private Long txNextIdSnapshot;
    private boolean replayingWal;
    private long nextBatchId = 1L;
    private Long currentBatchId;
    private int activeReaders;
    private long lastAppliedBatchId;
    private long latestCommittedBatchId;
    private boolean indexSidecarCurrent;
    private boolean checkpointRequested;
    private WriterContext writerContext;
    private CommittedState committedState;
    private final List<IndexWalOperation> pendingIndexOperations = new ArrayList<>();
    private final List<RecordRollbackOperation> pendingRecordRollbacks = new ArrayList<>();
    private final Set<String> pendingExactNamespaceOps = new LinkedHashSet<>();
    private final Set<String> pendingOrderedNamespaceOps = new LinkedHashSet<>();
    private final Set<String> pendingVectorNamespaceOps = new LinkedHashSet<>();

    @Override
    public synchronized void open(StorageConfig config) {
        close();
        this.config = config;
        int pageSize = Math.toIntExact(config.pagePoolSize() > 0 ? config.pagePoolSize() : 8192L);
        if (!config.memoryOnly()) {
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
        }
        NativeStorageState metadata = config.memoryOnly()
                ? new NativeStorageState(null, Map.of(), Map.of(), 1L, 0L)
                : StorageSnapshotCodec.decode(readPagePayload());
        root = metadata.root();
        schemaRegistry = new InMemorySchemaRegistry(metadata.schemas());
        lastAppliedBatchId = metadata.lastAppliedBatchId();
        latestCommittedBatchId = lastAppliedBatchId;
        indexManager = new ExactMatchIndexManager();
        orderedIndexManager = new OrderedRangeIndexManager();
        vectorIndexManager = new VectorIndexManager();
        if (indexStore != null) {
            try {
                indexStore.loadInto(indexManager, orderedIndexManager);
            } catch (RuntimeException e) {
                indexManager.clear();
                orderedIndexManager.clear();
            }
        }
        recordStore = config.memoryOnly()
                ? new InMemoryRecordStore(this::recordRollbackOperation)
                : new PageBackedRecordStore(
                        Path.of(config.path() + ".records"),
                        pageSize,
                        this::resolveEntityType,
                        this::recordRollbackOperation
                );
        recordStore.open();
        if (writeAheadLog != null) {
            replayWriteAheadLog();
        }
        txRootSnapshot = null;
        txSchemaSnapshot = null;
        txNextIdSnapshot = null;
        pendingIndexOperations.clear();
        pendingRecordRollbacks.clear();
        pendingExactNamespaceOps.clear();
        pendingOrderedNamespaceOps.clear();
        pendingVectorNamespaceOps.clear();
        replayingWal = false;
        currentBatchId = null;
        indexSidecarCurrent = true;
        checkpointRequested = !config.memoryOnly() && walSizeBytes() > config.maxWalBytes();
        writerContext = null;
        committedState = snapshotCommittedState();
    }

    @Override
    public synchronized boolean isOpen() {
        if (config != null && config.memoryOnly()) {
            return schemaRegistry != null && recordStore != null;
        }
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
        if (mode == TransactionMode.READ_ONLY) {
            activeReaders++;
            return new NativeReadOnlyTransaction();
        }
        if (currentBatchId != null) {
            throw new IllegalStateException("Native storage engine already has an active writer");
        }
        txRootSnapshot = root;
        txSchemaSnapshot = schemaRegistry.snapshot();
        txNextIdSnapshot = recordStore.nextId();
        currentBatchId = nextBatchId++;
        writerContext = new WriterContext(
                Thread.currentThread(),
                root,
                new InMemorySchemaRegistry(txSchemaSnapshot),
                new OverlayRecordStore(recordStore, txNextIdSnapshot)
        );
        pendingIndexOperations.clear();
        pendingRecordRollbacks.clear();
        pendingExactNamespaceOps.clear();
        pendingOrderedNamespaceOps.clear();
        pendingVectorNamespaceOps.clear();
        indexSidecarCurrent = false;
        recordStore.setAutoFlush(false);
        appendBatchEntry(WriteAheadLog.BEGIN, currentBatchId, mode.name().getBytes(StandardCharsets.UTF_8));
        return new NativeTransaction(mode);
    }

    public synchronized SchemaRegistry schemaRegistry() {
        ensureOpen();
        WriterContext context = currentWriterContext();
        if (context != null) {
            return context.schemaRegistry();
        }
        return currentBatchId != null ? committedState.schemaRegistry() : schemaRegistry;
    }

    public synchronized RecordStore recordStore() {
        ensureOpen();
        WriterContext context = currentWriterContext();
        if (context != null) {
            return context.recordStore();
        }
        return currentBatchId != null ? committedState.recordStore() : recordStore;
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
        if (!config.memoryOnly()) {
            indexSidecarCurrent = false;
        }
        return indexManager;
    }

    public synchronized OrderedRangeIndexManager orderedIndexManager() {
        ensureOpen();
        if (!config.memoryOnly()) {
            indexSidecarCurrent = false;
        }
        return orderedIndexManager;
    }

    public synchronized Set<Object> exactIndexFind(String namespace, String fieldName, FieldValue value) {
        ensureOpen();
        Set<Object> committed = currentBatchId != null
                ? committedState.exactIndexes().find(namespace, fieldName, value)
                : indexManager.find(namespace, fieldName, value);
        WriterContext context = currentWriterContext();
        if (context == null) {
            return committed;
        }
        LinkedHashSet<Object> merged = new LinkedHashSet<>(committed);
        for (IndexWalOperation operation : pendingIndexOperations) {
            if (!namespace.equals(operation.namespace())) {
                continue;
            }
            switch (operation.kind()) {
                case EXACT_ADD -> {
                    FieldValue candidate = operation.fieldValues().get(fieldName);
                    if (value.equals(candidate)) {
                        merged.add(operation.identifier());
                    }
                }
                case EXACT_REMOVE -> {
                    FieldValue candidate = operation.fieldValues().get(fieldName);
                    if (value.equals(candidate)) {
                        merged.remove(operation.identifier());
                    }
                }
                default -> {
                }
            }
        }
        return Set.copyOf(merged);
    }

    public synchronized Set<Object> orderedIndexRange(String namespace, String fieldName, FieldValue fromInclusive, FieldValue toInclusive) {
        ensureOpen();
        Set<Object> committed = currentBatchId != null
                ? committedState.orderedIndexes().range(namespace, fieldName, fromInclusive, toInclusive)
                : orderedIndexManager.range(namespace, fieldName, fromInclusive, toInclusive);
        WriterContext context = currentWriterContext();
        if (context == null) {
            return committed;
        }
        LinkedHashSet<Object> merged = new LinkedHashSet<>(committed);
        for (IndexWalOperation operation : pendingIndexOperations) {
            if (!namespace.equals(operation.namespace()) || !fieldName.equals(operation.fieldName()) || operation.fieldValue() == null) {
                continue;
            }
            int lower = OrderedRangeIndex.compare(operation.fieldValue(), fromInclusive);
            int upper = OrderedRangeIndex.compare(operation.fieldValue(), toInclusive);
            if (lower >= 0 && upper <= 0) {
                if (operation.kind() == IndexWalKind.ORDERED_ADD) {
                    merged.add(operation.identifier());
                } else if (operation.kind() == IndexWalKind.ORDERED_REMOVE) {
                    merged.remove(operation.identifier());
                }
            }
        }
        return Set.copyOf(merged);
    }

    public synchronized List<VectorMatch<Object>> vectorNearest(String namespace, String fieldName, Vector query, int limit) {
        ensureOpen();
        List<VectorMatch<Object>> committed = currentBatchId != null
                ? committedState.vectorIndexes().nearest(namespace, fieldName, query, limit)
                : vectorIndexManager.nearest(namespace, fieldName, query, limit);
        WriterContext context = currentWriterContext();
        if (context == null) {
            return committed;
        }
        LinkedHashMap<Object, Float> merged = new LinkedHashMap<>();
        for (VectorMatch<Object> match : committed) {
            merged.put(match.id(), match.distance());
        }
        for (IndexWalOperation operation : pendingIndexOperations) {
            if (!namespace.equals(operation.namespace())
                    || !fieldName.equals(operation.fieldName())
                    || operation.vectorValue() == null) {
                continue;
            }
            switch (operation.kind()) {
                case VECTOR_ADD -> merged.put(operation.identifier(), operation.distanceMetric().distance(query, operation.vectorValue()));
                case VECTOR_REMOVE -> merged.remove(operation.identifier());
                default -> {
                }
            }
        }
        List<VectorMatch<Object>> matches = new ArrayList<>(merged.size());
        for (Map.Entry<Object, Float> entry : merged.entrySet()) {
            matches.add(new VectorMatch<>(entry.getKey(), entry.getValue()));
        }
        matches.sort(java.util.Comparator.comparing(VectorMatch::distance));
        return List.copyOf(matches.subList(0, Math.min(limit, matches.size())));
    }

    @Override
    public synchronized long walSizeBytes() {
        ensureOpen();
        if (writeAheadLog == null) {
            return 0L;
        }
        return writeAheadLog.sizeBytes();
    }

    @Override
    public synchronized boolean needsCheckpoint() {
        ensureOpen();
        return !config.memoryOnly() && checkpointRequested;
    }

    @Override
    public synchronized void checkpoint() {
        ensureOpen();
        if (config.memoryOnly()) {
            checkpointRequested = false;
            return;
        }
        if (currentBatchId != null || activeReaders > 0) {
            throw new IllegalStateException("Cannot checkpoint with an active transaction");
        }
        recordStore.setAutoFlush(true);
        recordStore.flush();
        persistState(true);
    }

    public synchronized <I> void ensureExactMatchNamespace(
            String namespace,
            Collection<String> indexedFields,
            EntityType entityType,
            Function<Record, I> identifierExtractor
    ) {
        ensureOpen();
        if (currentWriterContext() != null) {
            recordExactNamespaceOperation(namespace, indexedFields);
            return;
        }
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
        if (currentWriterContext() != null) {
            recordOrderedNamespaceOperation(namespace, indexedFields);
            return;
        }
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

    public synchronized <I> void ensureVectorNamespace(
            String namespace,
            Collection<VectorIndexDefinition> definitions,
            EntityType entityType,
            Function<Record, I> identifierExtractor
    ) {
        ensureOpen();
        if (currentWriterContext() != null) {
            recordVectorNamespaceOperation(namespace, definitions);
            return;
        }
        if (vectorIndexManager.ensureNamespace(namespace, definitions)) {
            return;
        }
        recordVectorNamespaceOperation(namespace, definitions);
        for (Record record : recordStore.scan(entityType)) {
            I identifier = identifierExtractor.apply(record);
            if (identifier == null) {
                continue;
            }
            for (VectorIndexDefinition definition : definitions) {
                FieldValue value = record.fields().get(definition.field());
                if (value instanceof VectorValue vectorValue) {
                    vectorIndexManager.add(namespace, definition.field(), vectorValue.vector(), identifier);
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
        if (currentWriterContext() != null && currentBatchId != null && !replayingWal) {
            recordExactNamespaceOperation(namespace, indexedFields);
            pendingIndexOperations.add(IndexWalOperation.exactAdd(namespace, List.copyOf(indexedFields), fieldValues, identifier));
            return;
        }
        if (!indexManager.ensureNamespace(namespace, indexedFields)) {
            recordExactNamespaceOperation(namespace, indexedFields);
        } else if (!replayingWal && indexStore != null) {
            // namespace already exists; nothing to persist here
        }
        indexManager.add(namespace, fieldValues, identifier);
        if (!replayingWal && indexStore != null) {
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
        if (currentWriterContext() != null && currentBatchId != null && !replayingWal) {
            recordExactNamespaceOperation(namespace, indexedFields);
            pendingIndexOperations.add(IndexWalOperation.exactRemove(namespace, List.copyOf(indexedFields), fieldValues, identifier));
            return;
        }
        if (!indexManager.ensureNamespace(namespace, indexedFields)) {
            recordExactNamespaceOperation(namespace, indexedFields);
        } else if (!replayingWal && indexStore != null) {
            // namespace already exists; nothing to persist here
        }
        indexManager.remove(namespace, fieldValues, identifier);
        if (!replayingWal && indexStore != null) {
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
        if (currentWriterContext() != null && currentBatchId != null && !replayingWal) {
            recordOrderedNamespaceOperation(namespace, indexedFields);
            pendingIndexOperations.add(IndexWalOperation.orderedAdd(namespace, fieldName, value, identifier));
            return;
        }
        if (!orderedIndexManager.ensureNamespace(namespace, indexedFields)) {
            recordOrderedNamespaceOperation(namespace, indexedFields);
        } else if (!replayingWal && indexStore != null) {
            // namespace already exists; nothing to persist here
        }
        orderedIndexManager.add(namespace, fieldName, value, identifier);
        if (!replayingWal && indexStore != null) {
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
        if (currentWriterContext() != null && currentBatchId != null && !replayingWal) {
            recordOrderedNamespaceOperation(namespace, indexedFields);
            pendingIndexOperations.add(IndexWalOperation.orderedRemove(namespace, fieldName, value, identifier));
            return;
        }
        if (!orderedIndexManager.ensureNamespace(namespace, indexedFields)) {
            recordOrderedNamespaceOperation(namespace, indexedFields);
        } else if (!replayingWal && indexStore != null) {
            // namespace already exists; nothing to persist here
        }
        orderedIndexManager.remove(namespace, fieldName, value, identifier);
        if (!replayingWal && indexStore != null) {
            try {
                indexStore.applyOrderedRemove(namespace, fieldName, value, identifier);
            } catch (IOException e) {
                throw new RuntimeException("Failed to update native ordered index store", e);
            }
        }
    }

    public synchronized void vectorIndexAdd(
            String namespace,
            Collection<VectorIndexDefinition> definitions,
            String fieldName,
            Vector value,
            Object identifier,
            String distanceMetric
    ) {
        ensureOpen();
        if (currentWriterContext() != null && currentBatchId != null && !replayingWal) {
            recordVectorNamespaceOperation(namespace, definitions);
            pendingIndexOperations.add(IndexWalOperation.vectorAdd(namespace, fieldName, value, identifier, distanceMetric));
            return;
        }
        if (!vectorIndexManager.ensureNamespace(namespace, definitions)) {
            recordVectorNamespaceOperation(namespace, definitions);
        }
        vectorIndexManager.add(namespace, fieldName, value, identifier);
    }

    public synchronized void vectorIndexRemove(
            String namespace,
            Collection<VectorIndexDefinition> definitions,
            String fieldName,
            Object identifier,
            String distanceMetric
    ) {
        ensureOpen();
        if (currentWriterContext() != null && currentBatchId != null && !replayingWal) {
            recordVectorNamespaceOperation(namespace, definitions);
            pendingIndexOperations.add(IndexWalOperation.vectorRemove(namespace, fieldName, identifier, definitions, distanceMetric));
            return;
        }
        if (!vectorIndexManager.ensureNamespace(namespace, definitions)) {
            recordVectorNamespaceOperation(namespace, definitions);
        }
        vectorIndexManager.remove(namespace, fieldName, identifier);
    }

    @Override
    public synchronized void close() {
        if (isOpen()) {
            if (currentBatchId != null) {
                rollbackPendingIndexState();
                writerContext = null;
            }
            if (config.checkpointOnClose()) {
                checkpoint();
            } else {
                recordStore.setAutoFlush(true);
                recordStore.flush();
            }
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
        if (vectorIndexManager != null) {
            vectorIndexManager.clear();
        }
        vectorIndexManager = null;
        root = null;
        txRootSnapshot = null;
        txSchemaSnapshot = null;
        txNextIdSnapshot = null;
        pendingIndexOperations.clear();
        pendingRecordRollbacks.clear();
        pendingExactNamespaceOps.clear();
        pendingOrderedNamespaceOps.clear();
        pendingVectorNamespaceOps.clear();
        config = null;
        currentBatchId = null;
        activeReaders = 0;
        lastAppliedBatchId = 0L;
        latestCommittedBatchId = 0L;
        indexSidecarCurrent = false;
        checkpointRequested = false;
        writerContext = null;
        committedState = null;
    }

    private void ensureOpen() {
        if (!isOpen()) {
            throw new IllegalStateException("Native storage engine is not open");
        }
    }

    private WriterContext currentWriterContext() {
        if (writerContext == null) {
            return null;
        }
        return writerContext.ownerThread() == Thread.currentThread() ? writerContext : null;
    }

    private Object currentRootValue() {
        WriterContext context = currentWriterContext();
        if (context != null) {
            return context.root();
        }
        return currentBatchId != null ? committedState.root() : root;
    }

    private void setCurrentRootValue(Object updatedRoot) {
        WriterContext context = currentWriterContext();
        if (context != null) {
            context.setRoot(updatedRoot);
        } else {
            root = updatedRoot;
            committedState = snapshotCommittedState();
        }
    }

    private Map<String, SchemaDefinition> currentSchemaSnapshot() {
        WriterContext context = currentWriterContext();
        return context != null ? context.schemaRegistry().snapshot() : schemaRegistry.snapshot();
    }

    private long currentNextIdValue() {
        WriterContext context = currentWriterContext();
        return context != null ? context.recordStore().nextId() : recordStore.nextId();
    }

    private CommittedState snapshotCommittedState() {
        return new CommittedState(
                root,
                new InMemorySchemaRegistry(schemaRegistry.snapshot()),
                indexManager.copy(),
                orderedIndexManager.copy(),
                vectorIndexManager.copy(),
                new SnapshotRecordStore(recordStore.snapshot())
        );
    }

    private byte[] readPagePayload() {
        if (pageFile == null) {
            return new byte[0];
        }
        try {
            return pageFile.readPayload();
        } catch (IOException e) {
            throw new RuntimeException("Failed to read native storage payload", e);
        }
    }

    private void persistState() {
        persistState(true);
    }

    private void persistState(boolean rewriteIndexSidecar) {
        if (config != null && config.memoryOnly()) {
            checkpointRequested = false;
            indexSidecarCurrent = true;
            return;
        }
        try {
            lastAppliedBatchId = latestCommittedBatchId;
            persistMetadataState();
            if (rewriteIndexSidecar) {
                persistIndexState();
            }
            writeAheadLog.clear();
            indexSidecarCurrent = true;
            checkpointRequested = false;
            committedState = snapshotCommittedState();
        } catch (IOException e) {
            throw new RuntimeException("Failed to persist native storage state", e);
        }
    }

    private void persistMetadataState() throws IOException {
        if (pageFile == null) {
            return;
        }
        NativeStorageState metadataState = new NativeStorageState(root, schemaRegistry.snapshot(), java.util.Map.of(), recordStore.nextId(), lastAppliedBatchId);
        pageFile.writePayload(StorageSnapshotCodec.encode(metadataState));
    }

    private void persistIndexState() throws IOException {
        if (indexStore != null && indexManager != null && orderedIndexManager != null) {
            indexStore.persist(indexManager, orderedIndexManager);
        }
    }

    private void recordExactNamespaceOperation(String namespace, Collection<String> indexedFields) {
        if (currentBatchId != null && !replayingWal && pendingExactNamespaceOps.add(namespace)) {
            pendingIndexOperations.add(IndexWalOperation.exactNamespace(namespace, List.copyOf(indexedFields)));
        }
    }

    private void recordOrderedNamespaceOperation(String namespace, Collection<String> indexedFields) {
        if (currentBatchId != null && !replayingWal && pendingOrderedNamespaceOps.add(namespace)) {
            pendingIndexOperations.add(IndexWalOperation.orderedNamespace(namespace, List.copyOf(indexedFields)));
        }
    }

    private void recordVectorNamespaceOperation(String namespace, Collection<VectorIndexDefinition> definitions) {
        if (currentBatchId != null && !replayingWal && pendingVectorNamespaceOps.add(namespace)) {
            pendingIndexOperations.add(IndexWalOperation.vectorNamespace(namespace, List.copyOf(definitions)));
        }
    }

    private void recordRollbackOperation(byte operationType, Record beforeRecord, Record afterRecord, RecordId recordId) {
        if (currentBatchId == null || replayingWal) {
            return;
        }
        pendingRecordRollbacks.add(RecordRollbackOperation.from(operationType, beforeRecord, afterRecord, recordId));
    }

    private void rollbackActiveTransaction() {
        replayingWal = true;
        try {
            recordStore.setAutoFlush(false);
            for (int i = pendingRecordRollbacks.size() - 1; i >= 0; i--) {
                pendingRecordRollbacks.get(i).rollback(recordStore);
            }
            if (txNextIdSnapshot != null) {
                recordStore.setNextId(txNextIdSnapshot);
            }
            recordStore.setAutoFlush(true);
            recordStore.flush();
            root = txRootSnapshot;
            schemaRegistry = new InMemorySchemaRegistry(txSchemaSnapshot == null ? Map.of() : txSchemaSnapshot);
        } finally {
            if (recordStore != null) {
                recordStore.setAutoFlush(true);
            }
            replayingWal = false;
        }
    }

    private void rollbackPendingIndexState() {
        indexSidecarCurrent = true;
    }

    private EntityType resolveEntityType(String name) {
        SchemaDefinition schema = schemaRegistry().schema(name);
        return schema != null ? schema.entityType() : new EntityType(name, List.of(), List.of());
    }

    synchronized void writeCommittedStateToWalForTest() {
        long batchId = requireCurrentBatchId();
        WriterContext context = currentWriterContext();
        if (context != null) {
            applyWriterOverlayToCommittedStore(context, false);
        }
        appendBatchEntry(WriteAheadLog.METADATA, batchId, encodeMetadataCheckpoint());
        if (context != null) {
            for (OverlayRecordStore.RecordMutation mutation : context.recordStore().mutations()) {
                if (mutation.operationType() == WriteAheadLog.UPSERT) {
                    appendBatchEntry(WriteAheadLog.UPSERT, batchId, RecordCodec.encode(mutation.record()));
                } else if (mutation.operationType() == WriteAheadLog.DELETE) {
                    appendBatchEntry(WriteAheadLog.DELETE, batchId, encodeDelete(mutation.recordId()));
                }
            }
        }
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
        WriterContext context = currentWriterContext();
        if (context != null) {
            applyWriterOverlayToCommittedStore(context, false);
        }
        appendBatchEntry(WriteAheadLog.METADATA, batchId, encodeMetadataCheckpoint());
        if (context != null) {
            boolean insertedForeign = false;
            for (OverlayRecordStore.RecordMutation mutation : context.recordStore().mutations()) {
                byte type = mutation.operationType();
                byte[] payload = type == WriteAheadLog.UPSERT
                        ? RecordCodec.encode(mutation.record())
                        : encodeDelete(mutation.recordId());
                if (!insertedForeign) {
                    appendBatchEntry(type, batchId + 1000, payload);
                    insertedForeign = true;
                }
                appendBatchEntry(type, batchId, payload);
            }
        }
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
        WriterContext context = currentWriterContext();
        if (context != null) {
            applyWriterOverlayToCommittedStore(context);
            root = context.root();
            schemaRegistry = new InMemorySchemaRegistry(context.schemaRegistry().snapshot());
        }
        recordStore.setAutoFlush(true);
        recordStore.flush();
        lastAppliedBatchId = batchId;
        latestCommittedBatchId = batchId;
        NativeStorageState metadataState = new NativeStorageState(root, schemaRegistry.snapshot(), java.util.Map.of(), recordStore.nextId(), lastAppliedBatchId);
        try {
            pageFile.writePayload(StorageSnapshotCodec.encode(metadataState));
            persistIndexState();
            indexSidecarCurrent = true;
            committedState = snapshotCommittedState();
        } catch (IOException e) {
            throw new RuntimeException("Failed to persist checkpoint without clearing WAL", e);
        }
    }

    synchronized void flushRecordStoreWithoutEngineCheckpointForTest() {
        WriterContext context = currentWriterContext();
        if (context != null) {
            applyWriterOverlayToCommittedStore(context);
            root = context.root();
            schemaRegistry = new InMemorySchemaRegistry(context.schemaRegistry().snapshot());
        }
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
        txRootSnapshot = null;
        txSchemaSnapshot = null;
        txNextIdSnapshot = null;
        pendingIndexOperations.clear();
        pendingRecordRollbacks.clear();
        pendingExactNamespaceOps.clear();
        pendingOrderedNamespaceOps.clear();
        config = null;
        replayingWal = false;
        currentBatchId = null;
        activeReaders = 0;
        lastAppliedBatchId = 0L;
        latestCommittedBatchId = 0L;
        indexSidecarCurrent = false;
        checkpointRequested = false;
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
        NativeStorageState metadataState = new NativeStorageState(currentRootValue(), currentSchemaSnapshot(), java.util.Map.of(), currentNextIdValue(), lastAppliedBatchId);
        return StorageSnapshotCodec.encode(metadataState);
    }

    private byte[] encodeDelete(RecordId recordId) {
        return ByteBuffer.allocate(Long.BYTES).putLong(recordId.value()).array();
    }

    private RecordId decodeDelete(byte[] payload) {
        return new RecordId(ByteBuffer.wrap(payload).getLong());
    }

    private void appendBatchEntry(byte type, long batchId, byte[] payload) {
        if (writeAheadLog == null) {
            return;
        }
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
                            latestCommittedBatchId = activeBatchId;
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
        } else if (isOpen()) {
            checkpointRequested = walSizeBytes() > config.maxWalBytes();
        }
    }

    private void applyRecoveredIndexState(List<IndexWalOperation> committedIndexOperations) {
        for (IndexWalOperation operation : committedIndexOperations) {
            applyIndexWalOperation(operation);
        }
    }

    private void applyPendingIndexOperationsToCommittedState() {
        for (IndexWalOperation operation : pendingIndexOperations) {
            applyIndexWalOperation(operation);
        }
    }

    private void applyRecoveredLogicalTransaction(NativeStorageState metadataState, List<WriteAheadLog.Entry> operations) {
        replayingWal = true;
        try {
            root = metadataState.root();
            schemaRegistry = new InMemorySchemaRegistry(metadataState.schemas());
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
            schemaRegistry = new InMemorySchemaRegistry(metadataState.schemas());
            recordStore.applyRecoveredPageDiffState(pageCount, recordSlotIndex, allocatorState, pageDiffs);
        } finally {
            replayingWal = false;
        }
    }

    private byte[] encodePageCount(int pageCount) {
        return ByteBuffer.allocate(Integer.BYTES).putInt(pageCount).array();
    }

    private void applyWriterOverlayToCommittedStore(WriterContext context) {
        applyWriterOverlayToCommittedStore(context, true);
    }

    private void applyWriterOverlayToCommittedStore(WriterContext context, boolean flush) {
        replayingWal = true;
        try {
            recordStore.setAutoFlush(false);
            for (OverlayRecordStore.RecordMutation mutation : context.recordStore().mutations()) {
                if (mutation.operationType() == WriteAheadLog.UPSERT) {
                    recordStore.recoverUpsert(mutation.record());
                } else if (mutation.operationType() == WriteAheadLog.DELETE) {
                    recordStore.recoverDelete(mutation.recordId());
                }
            }
            recordStore.setNextId(context.recordStore().nextId());
            if (flush) {
                recordStore.setAutoFlush(true);
                recordStore.flush();
            }
        } finally {
            replayingWal = false;
            if (flush) {
                recordStore.setAutoFlush(true);
            } else {
                recordStore.setAutoFlush(false);
            }
        }
    }

    private void applyIndexWalOperation(IndexWalOperation operation) {
        switch (operation.kind()) {
            case EXACT_NAMESPACE -> indexManager.ensureNamespace(operation.namespace(), operation.indexedFields());
            case EXACT_ADD -> {
                if (!indexManager.namespaces().contains(operation.namespace())) {
                    indexManager.ensureNamespace(operation.namespace(), operation.indexedFields());
                }
                indexManager.add(operation.namespace(), operation.fieldValues(), operation.identifier());
            }
            case EXACT_REMOVE -> {
                if (!indexManager.namespaces().contains(operation.namespace())) {
                    indexManager.ensureNamespace(operation.namespace(), operation.indexedFields());
                }
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
            case VECTOR_NAMESPACE -> vectorIndexManager.ensureNamespace(operation.namespace(), operation.vectorIndexDefinitions());
            case VECTOR_ADD -> {
                vectorIndexManager.ensureNamespace(operation.namespace(), operation.vectorIndexDefinitions());
                vectorIndexManager.add(operation.namespace(), operation.fieldName(), operation.vectorValue(), operation.identifier());
            }
            case VECTOR_REMOVE -> {
                vectorIndexManager.ensureNamespace(operation.namespace(), operation.vectorIndexDefinitions());
                vectorIndexManager.remove(operation.namespace(), operation.fieldName(), operation.identifier());
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
                case WriteAheadLog.VECTOR_INDEX_NAMESPACE -> IndexWalOperation.vectorNamespace(namespace, readVectorIndexDefinitions(in));
                case WriteAheadLog.VECTOR_INDEX_ADD -> IndexWalOperation.vectorAdd(
                        namespace,
                        in.readUTF(),
                        ((VectorValue) readFieldValue(in)).vector(),
                        readIdentifier(in),
                        readVectorIndexDefinitions(in),
                        in.readUTF()
                );
                case WriteAheadLog.VECTOR_INDEX_REMOVE -> IndexWalOperation.vectorRemove(
                        namespace,
                        in.readUTF(),
                        readIdentifier(in),
                        readVectorIndexDefinitions(in),
                        in.readUTF()
                );
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
                case VECTOR_NAMESPACE -> writeVectorIndexDefinitions(out, operation.vectorIndexDefinitions());
                case VECTOR_ADD -> {
                    out.writeUTF(operation.fieldName());
                    writeFieldValue(out, new VectorValue(operation.vectorValue()));
                    writeIdentifier(out, operation.identifier());
                    writeVectorIndexDefinitions(out, operation.vectorIndexDefinitions());
                    out.writeUTF(operation.distanceMetricName());
                }
                case VECTOR_REMOVE -> {
                    out.writeUTF(operation.fieldName());
                    writeIdentifier(out, operation.identifier());
                    writeVectorIndexDefinitions(out, operation.vectorIndexDefinitions());
                    out.writeUTF(operation.distanceMetricName());
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

    private void writeVectorIndexDefinitions(DataOutputStream out, List<VectorIndexDefinition> definitions) throws IOException {
        out.writeInt(definitions.size());
        for (VectorIndexDefinition definition : definitions) {
            out.writeUTF(definition.field());
            out.writeInt(definition.dimension());
            out.writeUTF(definition.distanceMetric());
        }
    }

    private List<VectorIndexDefinition> readVectorIndexDefinitions(DataInputStream in) throws IOException {
        int size = in.readInt();
        List<VectorIndexDefinition> definitions = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            definitions.add(new VectorIndexDefinition(in.readUTF(), in.readInt(), in.readUTF()));
        }
        return definitions;
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
        if (value instanceof VectorValue vectorValue) {
            out.writeByte(FIELD_VECTOR);
            Vector vector = vectorValue.vector();
            out.writeInt(vector.dimension());
            for (float component : vector.values()) {
                out.writeFloat(component);
            }
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
            case FIELD_VECTOR -> {
                int dimension = in.readInt();
                float[] values = new float[dimension];
                for (int i = 0; i < dimension; i++) {
                    values[i] = in.readFloat();
                }
                yield new VectorValue(new Vector(values));
            }
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
            List<VectorIndexDefinition> vectorIndexDefinitions,
            Map<String, FieldValue> fieldValues,
            String fieldName,
            FieldValue fieldValue,
            Vector vectorValue,
            biz.digitalindustry.db.vector.DistanceFunction distanceMetric,
            String distanceMetricName,
            Object identifier
    ) {
        static IndexWalOperation exactNamespace(String namespace, List<String> indexedFields) {
            return new IndexWalOperation(IndexWalKind.EXACT_NAMESPACE, namespace, indexedFields, List.of(), Map.of(), null, null, null, null, null, null);
        }

        static IndexWalOperation exactAdd(String namespace, Map<String, FieldValue> fieldValues, Object identifier) {
            return exactAdd(namespace, List.copyOf(fieldValues.keySet()), fieldValues, identifier);
        }

        static IndexWalOperation exactAdd(String namespace, List<String> indexedFields, Map<String, FieldValue> fieldValues, Object identifier) {
            return new IndexWalOperation(IndexWalKind.EXACT_ADD, namespace, indexedFields, List.of(), new LinkedHashMap<>(fieldValues), null, null, null, null, null, identifier);
        }

        static IndexWalOperation exactRemove(String namespace, Map<String, FieldValue> fieldValues, Object identifier) {
            return exactRemove(namespace, List.copyOf(fieldValues.keySet()), fieldValues, identifier);
        }

        static IndexWalOperation exactRemove(String namespace, List<String> indexedFields, Map<String, FieldValue> fieldValues, Object identifier) {
            return new IndexWalOperation(IndexWalKind.EXACT_REMOVE, namespace, indexedFields, List.of(), new LinkedHashMap<>(fieldValues), null, null, null, null, null, identifier);
        }

        static IndexWalOperation orderedNamespace(String namespace, List<String> indexedFields) {
            return new IndexWalOperation(IndexWalKind.ORDERED_NAMESPACE, namespace, indexedFields, List.of(), Map.of(), null, null, null, null, null, null);
        }

        static IndexWalOperation orderedAdd(String namespace, String fieldName, FieldValue fieldValue, Object identifier) {
            return orderedAdd(namespace, fieldName, fieldValue, identifier, List.of(fieldName));
        }

        static IndexWalOperation orderedAdd(String namespace, String fieldName, FieldValue fieldValue, Object identifier, List<String> indexedFields) {
            return new IndexWalOperation(IndexWalKind.ORDERED_ADD, namespace, indexedFields, List.of(), Map.of(), fieldName, fieldValue, null, null, null, identifier);
        }

        static IndexWalOperation orderedRemove(String namespace, String fieldName, FieldValue fieldValue, Object identifier) {
            return orderedRemove(namespace, fieldName, fieldValue, identifier, List.of(fieldName));
        }

        static IndexWalOperation orderedRemove(String namespace, String fieldName, FieldValue fieldValue, Object identifier, List<String> indexedFields) {
            return new IndexWalOperation(IndexWalKind.ORDERED_REMOVE, namespace, indexedFields, List.of(), Map.of(), fieldName, fieldValue, null, null, null, identifier);
        }

        static IndexWalOperation vectorNamespace(String namespace, List<VectorIndexDefinition> definitions) {
            return new IndexWalOperation(IndexWalKind.VECTOR_NAMESPACE, namespace, List.of(), definitions, Map.of(), null, null, null, null, null, null);
        }

        static IndexWalOperation vectorAdd(String namespace, String fieldName, Vector vectorValue, Object identifier, String distanceMetricName) {
            return vectorAdd(namespace, fieldName, vectorValue, identifier, List.of(new VectorIndexDefinition(fieldName, vectorValue.dimension(), distanceMetricName)), distanceMetricName);
        }

        static IndexWalOperation vectorAdd(
                String namespace,
                String fieldName,
                Vector vectorValue,
                Object identifier,
                List<VectorIndexDefinition> definitions,
                String distanceMetricName
        ) {
            return new IndexWalOperation(
                    IndexWalKind.VECTOR_ADD,
                    namespace,
                    List.of(),
                    List.copyOf(definitions),
                    Map.of(),
                    fieldName,
                    null,
                    vectorValue,
                    biz.digitalindustry.db.vector.Distances.resolve(distanceMetricName),
                    distanceMetricName,
                    identifier
            );
        }

        static IndexWalOperation vectorRemove(String namespace, String fieldName, Object identifier, Collection<VectorIndexDefinition> definitions, String distanceMetricName) {
            return new IndexWalOperation(
                    IndexWalKind.VECTOR_REMOVE,
                    namespace,
                    List.of(),
                    List.copyOf(definitions),
                    Map.of(),
                    fieldName,
                    null,
                    null,
                    biz.digitalindustry.db.vector.Distances.resolve(distanceMetricName),
                    distanceMetricName,
                    identifier
            );
        }

        byte entryType() {
            return switch (kind) {
                case EXACT_NAMESPACE -> WriteAheadLog.EXACT_INDEX_NAMESPACE;
                case EXACT_ADD -> WriteAheadLog.EXACT_INDEX_ADD;
                case EXACT_REMOVE -> WriteAheadLog.EXACT_INDEX_REMOVE;
                case ORDERED_NAMESPACE -> WriteAheadLog.ORDERED_INDEX_NAMESPACE;
                case ORDERED_ADD -> WriteAheadLog.ORDERED_INDEX_ADD;
                case ORDERED_REMOVE -> WriteAheadLog.ORDERED_INDEX_REMOVE;
                case VECTOR_NAMESPACE -> WriteAheadLog.VECTOR_INDEX_NAMESPACE;
                case VECTOR_ADD -> WriteAheadLog.VECTOR_INDEX_ADD;
                case VECTOR_REMOVE -> WriteAheadLog.VECTOR_INDEX_REMOVE;
            };
        }
    }

    private enum IndexWalKind {
        EXACT_NAMESPACE,
        EXACT_ADD,
        EXACT_REMOVE,
        ORDERED_NAMESPACE,
        ORDERED_ADD,
        ORDERED_REMOVE,
        VECTOR_NAMESPACE,
        VECTOR_ADD,
        VECTOR_REMOVE
    }

    private record RecordRollbackOperation(Kind kind, Record record, RecordId recordId) {
        static RecordRollbackOperation from(byte operationType, Record beforeRecord, Record afterRecord, RecordId recordId) {
            if (operationType == WriteAheadLog.DELETE) {
                return new RecordRollbackOperation(Kind.RESTORE, beforeRecord, recordId);
            }
            if (beforeRecord == null && afterRecord != null) {
                return new RecordRollbackOperation(Kind.DELETE, null, recordId);
            }
            return new RecordRollbackOperation(Kind.RESTORE, beforeRecord, recordId);
        }

        void rollback(ManagedRecordStore recordStore) {
            if (kind == Kind.DELETE) {
                recordStore.recoverDelete(recordId);
            } else if (record != null) {
                recordStore.recoverUpsert(record);
            }
        }

        private enum Kind {
            RESTORE,
            DELETE
        }
    }

    private static final class WriterContext {
        private final Thread ownerThread;
        private Object root;
        private final InMemorySchemaRegistry schemaRegistry;
        private final OverlayRecordStore recordStore;

        private WriterContext(
                Thread ownerThread,
                Object root,
                InMemorySchemaRegistry schemaRegistry,
                OverlayRecordStore recordStore
        ) {
            this.ownerThread = ownerThread;
            this.root = root;
            this.schemaRegistry = schemaRegistry;
            this.recordStore = recordStore;
        }

        private Thread ownerThread() {
            return ownerThread;
        }

        private Object root() {
            return root;
        }

        private void setRoot(Object root) {
            this.root = root;
        }

        private InMemorySchemaRegistry schemaRegistry() {
            return schemaRegistry;
        }

        private OverlayRecordStore recordStore() {
            return recordStore;
        }
    }

    private record CommittedState(
            Object root,
            InMemorySchemaRegistry schemaRegistry,
            ExactMatchIndexManager exactIndexes,
            OrderedRangeIndexManager orderedIndexes,
            VectorIndexManager vectorIndexes,
            SnapshotRecordStore recordStore
    ) {
    }

    private final class NativeRootHandle implements RootHandle {
        @Override
        public Object get() {
            return currentRootValue();
        }

        @Override
        public <T> T get(Class<T> type) {
            Object current = currentRootValue();
            return type.isInstance(current) ? type.cast(current) : null;
        }

        @Override
        public void set(Object root) {
            setCurrentRootValue(root);
        }
    }

    private final class NativeReadOnlyTransaction implements Transaction {
        private boolean active = true;

        @Override
        public void commit() {
            close();
        }

        @Override
        public void rollback() {
            close();
        }

        @Override
        public void close() {
            if (active) {
                synchronized (NativeStorageEngine.this) {
                    activeReaders--;
                }
                active = false;
            }
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
                WriterContext context = currentWriterContext();
                if (context == null) {
                    throw new IllegalStateException("Missing writer context for active transaction");
                }
                appendBatchEntry(WriteAheadLog.METADATA, batchId, encodeMetadataCheckpoint());
                for (OverlayRecordStore.RecordMutation mutation : context.recordStore().mutations()) {
                    if (mutation.operationType() == WriteAheadLog.UPSERT) {
                        appendBatchEntry(WriteAheadLog.UPSERT, batchId, RecordCodec.encode(mutation.record()));
                    } else if (mutation.operationType() == WriteAheadLog.DELETE) {
                        appendBatchEntry(WriteAheadLog.DELETE, batchId, encodeDelete(mutation.recordId()));
                    }
                }
                appendIndexWalEntries(batchId);
                appendBatchEntry(WriteAheadLog.COMMIT, batchId, mode.name().getBytes(StandardCharsets.UTF_8));
                applyWriterOverlayToCommittedStore(context);
                root = context.root();
                schemaRegistry = new InMemorySchemaRegistry(context.schemaRegistry().snapshot());
                applyPendingIndexOperationsToCommittedState();
                committedState = snapshotCommittedState();
                latestCommittedBatchId = batchId;
                try {
                    persistMetadataState();
                    checkpointRequested = writeAheadLog != null && writeAheadLog.sizeBytes() > config.maxWalBytes();
                } catch (IOException e) {
                    throw new RuntimeException("Failed to persist committed native storage state", e);
                }
                txRootSnapshot = null;
                txSchemaSnapshot = null;
                txNextIdSnapshot = null;
                pendingIndexOperations.clear();
                pendingRecordRollbacks.clear();
                pendingExactNamespaceOps.clear();
                pendingOrderedNamespaceOps.clear();
                pendingVectorNamespaceOps.clear();
                currentBatchId = null;
                writerContext = null;
                active = false;
            }
        }

        @Override
        public void rollback() {
            if (active) {
                appendBatchEntry(WriteAheadLog.ROLLBACK, requireCurrentBatchId(), mode.name().getBytes(StandardCharsets.UTF_8));
                rollbackPendingIndexState();
                txRootSnapshot = null;
                txSchemaSnapshot = null;
                txNextIdSnapshot = null;
                pendingIndexOperations.clear();
                pendingRecordRollbacks.clear();
                pendingExactNamespaceOps.clear();
                pendingOrderedNamespaceOps.clear();
                pendingVectorNamespaceOps.clear();
                currentBatchId = null;
                writerContext = null;
                indexSidecarCurrent = true;
                active = false;
            }
        }

        @Override
        public void close() {
            rollback();
        }
    }
}
