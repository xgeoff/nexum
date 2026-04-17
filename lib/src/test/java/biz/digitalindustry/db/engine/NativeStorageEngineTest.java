package biz.digitalindustry.db.engine;

import biz.digitalindustry.db.engine.api.StorageConfig;
import biz.digitalindustry.db.engine.log.WriteAheadLog;
import biz.digitalindustry.db.model.LongValue;
import biz.digitalindustry.db.model.Record;
import biz.digitalindustry.db.model.StringValue;
import biz.digitalindustry.db.engine.page.PageFile;
import biz.digitalindustry.db.schema.EntityType;
import biz.digitalindustry.db.schema.FieldDefinition;
import biz.digitalindustry.db.schema.IndexDefinition;
import biz.digitalindustry.db.schema.IndexKind;
import biz.digitalindustry.db.schema.ValueType;
import biz.digitalindustry.db.engine.record.PageBackedRecordStore;
import biz.digitalindustry.db.engine.tx.Transaction;
import biz.digitalindustry.db.engine.tx.TransactionMode;
import org.junit.After;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;

public class NativeStorageEngineTest {
    private static final byte DEPRECATED_RECORD_STORE_METADATA_ENTRY = 8;
    private static final byte DEPRECATED_RECORD_PAGE_ENTRY = 9;
    private static final byte DEPRECATED_INDEX_SNAPSHOT_ENTRY = 13;

    private final NativeStorageEngine engine = new NativeStorageEngine();
    private Path dbPath;

    @After
    public void tearDown() throws Exception {
        engine.close();
        if (dbPath != null) {
            Files.deleteIfExists(Path.of(dbPath + ".indexes"));
            Files.deleteIfExists(Path.of(dbPath + ".records"));
            Files.deleteIfExists(Path.of(dbPath + ".wal"));
            Files.deleteIfExists(dbPath);
        }
    }

    @Test
    public void testOpenRootSchemaAndRecordStore() throws Exception {
        dbPath = Files.createTempFile("native-storage-engine", ".dbs");
        Files.deleteIfExists(dbPath);

        engine.open(new StorageConfig(dbPath.toString(), 8192));

        assertTrue(engine.isOpen());
        assertNotNull(engine.pageFile());
        assertNotNull(engine.writeAheadLog());
        assertNotNull(engine.schemaRegistry());
        assertNotNull(engine.recordStore());

        engine.root().set("root-object");
        assertEquals("root-object", engine.root().get(String.class));
    }

    @Test
    public void testMemoryOnlyModeKeepsStateWithoutDiskArtifacts() {
        engine.open(new StorageConfig("memory:test", 8192, StorageConfig.DEFAULT_MAX_WAL_BYTES, false, true));

        EntityType person = new EntityType(
                "Person",
                List.of(new FieldDefinition("age", ValueType.LONG, false, false)),
                List.of(new IndexDefinition("person_pk", IndexKind.PRIMARY, List.of("id")))
        );
        engine.schemaRegistry().register(person);

        try (Transaction tx = engine.begin(TransactionMode.READ_WRITE)) {
            engine.root().set("memory-root");
            engine.recordStore().create(person, new Record(null, person, Map.of("age", new LongValue(9L))));
            tx.commit();
        }

        assertEquals("memory-root", engine.root().get(String.class));
        assertFalse(engine.needsCheckpoint());
        assertEquals(0L, engine.walSizeBytes());
        assertTrue(engine.recordStore().scan(person).iterator().hasNext());
    }

    @Test
    public void testSchemaRegistrationAndRecordLifecycle() throws Exception {
        dbPath = Files.createTempFile("native-storage-records", ".dbs");
        Files.deleteIfExists(dbPath);
        engine.open(new StorageConfig(dbPath.toString(), 8192));

        EntityType person = new EntityType(
                "Person",
                List.of(new FieldDefinition("age", ValueType.LONG, false, false)),
                List.of(new IndexDefinition("person_pk", IndexKind.PRIMARY, List.of("id")))
        );
        engine.schemaRegistry().register(person);
        assertEquals(person, engine.schemaRegistry().entityType("Person"));

        Record created;
        try (Transaction tx = engine.begin(TransactionMode.READ_WRITE)) {
            created = engine.recordStore().create(person, new Record(null, person, Map.of("age", new LongValue(42))));
            tx.commit();
        }

        assertNotNull(created.id());
        assertEquals(created, engine.recordStore().get(created.id()));

        Record updated = new Record(created.id(), person, Map.of("age", new LongValue(43)));
        engine.recordStore().update(updated);
        assertEquals(updated, engine.recordStore().get(created.id()));
        assertTrue(engine.recordStore().delete(created.id()));
        assertNull(engine.recordStore().get(created.id()));
    }

    @Test
    public void testReopenPreservesRootSchemaAndRecords() throws Exception {
        dbPath = Files.createTempFile("native-storage-reopen", ".dbs");
        Files.deleteIfExists(dbPath);

        EntityType person = new EntityType(
                "Person",
                List.of(new FieldDefinition("age", ValueType.LONG, false, false)),
                List.of(new IndexDefinition("person_pk", IndexKind.PRIMARY, List.of("id")))
        );

        engine.open(new StorageConfig(dbPath.toString(), 8192));
        engine.root().set("persistent-root");
        engine.schemaRegistry().register(person);
        Record created;
        try (Transaction tx = engine.begin(TransactionMode.READ_WRITE)) {
            created = engine.recordStore().create(person, new Record(null, person, Map.of("age", new LongValue(7))));
            tx.commit();
        }
        engine.close();

        engine.open(new StorageConfig(dbPath.toString(), 8192));
        assertEquals("persistent-root", engine.root().get(String.class));
        assertNotNull(engine.schemaRegistry().entityType("Person"));

        Record reopened = engine.recordStore().get(created.id());
        assertNotNull(reopened);
        assertEquals(created.id(), reopened.id());
        assertEquals(new LongValue(7), reopened.fields().get("age"));
    }

    @Test
    public void testReopenPreservesHundredsOfRecordsWithoutMetadataOverflow() throws Exception {
        dbPath = Files.createTempFile("native-storage-large-metadata", ".dbs");
        Files.deleteIfExists(dbPath);

        EntityType person = new EntityType(
                "Person",
                List.of(new FieldDefinition("age", ValueType.LONG, false, false)),
                List.of(new IndexDefinition("person_pk", IndexKind.PRIMARY, List.of("id")))
        );

        engine.open(new StorageConfig(dbPath.toString(), 8192));
        engine.schemaRegistry().register(person);
        for (int i = 0; i < 500; i++) {
            try (Transaction tx = engine.begin(TransactionMode.READ_WRITE)) {
                engine.recordStore().create(person, new Record(null, person, Map.of("age", new LongValue(i))));
                tx.commit();
            }
        }
        engine.close();

        engine.open(new StorageConfig(dbPath.toString(), 8192));
        int count = 0;
        for (Record ignored : engine.recordStore().scan(person)) {
            count++;
        }
        assertEquals(500, count);
    }

    @Test
    public void testRollbackRestoresPreviousState() throws Exception {
        dbPath = Files.createTempFile("native-storage-rollback", ".dbs");
        Files.deleteIfExists(dbPath);
        engine.open(new StorageConfig(dbPath.toString(), 8192));

        EntityType person = new EntityType(
                "Person",
                List.of(new FieldDefinition("age", ValueType.LONG, false, false)),
                List.of(new IndexDefinition("person_pk", IndexKind.PRIMARY, List.of("id")))
        );
        engine.schemaRegistry().register(person);

        try (Transaction tx = engine.begin(TransactionMode.READ_WRITE)) {
            engine.root().set("before-rollback");
            engine.recordStore().create(person, new Record(null, person, Map.of("age", new LongValue(99))));
            tx.rollback();
        }

        assertNull(engine.root().get());
        assertFalse(engine.recordStore().scan(person).iterator().hasNext());
    }

    @Test
    public void testRollbackUndoesCreateUpdateAndDeleteWithoutFullSnapshot() throws Exception {
        dbPath = Files.createTempFile("native-storage-rollback-write-set", ".dbs");
        Files.deleteIfExists(dbPath);
        engine.open(new StorageConfig(dbPath.toString(), 8192));

        EntityType person = new EntityType(
                "Person",
                List.of(new FieldDefinition("age", ValueType.LONG, false, false)),
                List.of(new IndexDefinition("person_pk", IndexKind.PRIMARY, List.of("id")))
        );
        engine.schemaRegistry().register(person);

        Record created;
        try (Transaction tx = engine.begin(TransactionMode.READ_WRITE)) {
            created = engine.recordStore().create(person, new Record(null, person, Map.of("age", new LongValue(10))));
            tx.commit();
        }

        try (Transaction tx = engine.begin(TransactionMode.READ_WRITE)) {
            engine.root().set("rolled-back-root");
            engine.schemaRegistry().register(new EntityType("Transient", List.of(), List.of()));
            engine.recordStore().update(new Record(created.id(), person, Map.of("age", new LongValue(20))));
            engine.recordStore().create(person, new Record(null, person, Map.of("age", new LongValue(30))));
            engine.recordStore().delete(created.id());
            tx.rollback();
        }

        assertNull(engine.root().get());
        assertNull(engine.schemaRegistry().entityType("Transient"));
        Record restored = engine.recordStore().get(created.id());
        assertNotNull(restored);
        assertEquals(new LongValue(10), restored.fields().get("age"));
        int count = 0;
        for (Record ignored : engine.recordStore().scan(person)) {
            count++;
        }
        assertEquals(1, count);
    }

    @Test
    public void testRollbackUndoesPendingIndexMutationsWithoutIndexSnapshots() throws Exception {
        dbPath = Files.createTempFile("native-storage-index-rollback", ".dbs");
        Files.deleteIfExists(dbPath);
        engine.open(new StorageConfig(dbPath.toString(), 8192));

        try (Transaction tx = engine.begin(TransactionMode.READ_WRITE)) {
            engine.exactIndexAdd(
                    "object:people",
                    List.of("name"),
                    Map.of("name", new StringValue("Ada")),
                    "person-1"
            );
            engine.orderedIndexAdd(
                    "table:users",
                    List.of("age"),
                    "age",
                    new LongValue(37L),
                    "u1"
            );
            tx.commit();
        }

        try (Transaction tx = engine.begin(TransactionMode.READ_WRITE)) {
            engine.exactIndexAdd(
                    "object:people",
                    List.of("name"),
                    Map.of("name", new StringValue("Grace")),
                    "person-2"
            );
            engine.exactIndexRemove(
                    "object:people",
                    List.of("name"),
                    Map.of("name", new StringValue("Ada")),
                    "person-1"
            );
            engine.orderedIndexRemove(
                    "table:users",
                    List.of("age"),
                    "age",
                    new LongValue(37L),
                    "u1"
            );
            tx.rollback();
        }

        assertEquals(java.util.Set.of("person-1"), engine.exactIndexFind("object:people", "name", new StringValue("Ada")));
        assertEquals(java.util.Set.of(), engine.exactIndexFind("object:people", "name", new StringValue("Grace")));
        assertEquals(java.util.Set.of("u1"), engine.orderedIndexRange("table:users", "age", new LongValue(30L), new LongValue(40L)));
    }

    @Test
    public void testReaderDuringWriteSeesLastCommittedStateOnly() throws Exception {
        dbPath = Files.createTempFile("native-storage-overlap", ".dbs");
        Files.deleteIfExists(dbPath);
        engine.open(new StorageConfig(dbPath.toString(), 8192));

        EntityType person = new EntityType(
                "Person",
                List.of(new FieldDefinition("age", ValueType.LONG, false, false)),
                List.of(new IndexDefinition("person_pk", IndexKind.PRIMARY, List.of("id")))
        );
        engine.schemaRegistry().register(person);

        Record created;
        try (Transaction tx = engine.begin(TransactionMode.READ_WRITE)) {
            created = engine.recordStore().create(person, new Record(null, person, Map.of("age", new LongValue(10))));
            tx.commit();
        }

        CountDownLatch readerDone = new CountDownLatch(1);
        AtomicReference<LongValue> observed = new AtomicReference<>();

        try (Transaction tx = engine.begin(TransactionMode.READ_WRITE)) {
            engine.recordStore().update(new Record(created.id(), person, Map.of("age", new LongValue(20))));

            Thread reader = new Thread(() -> {
                try (Transaction ignored = engine.begin(TransactionMode.READ_ONLY)) {
                    Record seen = engine.recordStore().get(created.id());
                    observed.set((LongValue) seen.fields().get("age"));
                } finally {
                    readerDone.countDown();
                }
            });
            reader.start();

            assertTrue(readerDone.await(2, TimeUnit.SECONDS));
            assertEquals(new LongValue(10), observed.get());
            tx.commit();
        }

        assertEquals(new LongValue(20), engine.recordStore().get(created.id()).fields().get("age"));
    }

    @Test
    public void testRecoveryReplaysCommittedWalTransaction() throws Exception {
        dbPath = Files.createTempFile("native-storage-recovery", ".dbs");
        Files.deleteIfExists(dbPath);

        EntityType person = new EntityType(
                "Person",
                List.of(new FieldDefinition("age", ValueType.LONG, false, false)),
                List.of(new IndexDefinition("person_pk", IndexKind.PRIMARY, List.of("id")))
        );

        engine.open(new StorageConfig(dbPath.toString(), 8192));
        Record created;
        engine.begin(TransactionMode.READ_WRITE);
        engine.root().set("recovered-root");
        engine.schemaRegistry().register(person);
        created = engine.recordStore().create(person, new Record(null, person, Map.of("age", new LongValue(51))));
        engine.writeCommittedStateToWalForTest();
        engine.closeWithoutCheckpointForTest();

        engine.open(new StorageConfig(dbPath.toString(), 8192));
        assertEquals("recovered-root", engine.root().get(String.class));
        assertNotNull(engine.schemaRegistry().entityType("Person"));
        assertEquals(new LongValue(51), engine.recordStore().get(created.id()).fields().get("age"));
    }

    @Test
    public void testRecoveryIgnoresUncommittedWalTransaction() throws Exception {
        dbPath = Files.createTempFile("native-storage-uncommitted", ".dbs");
        Files.deleteIfExists(dbPath);

        EntityType person = new EntityType(
                "Person",
                List.of(new FieldDefinition("age", ValueType.LONG, false, false)),
                List.of(new IndexDefinition("person_pk", IndexKind.PRIMARY, List.of("id")))
        );

        engine.open(new StorageConfig(dbPath.toString(), 8192));
        engine.begin(TransactionMode.READ_WRITE);
        engine.root().set("should-not-survive");
        engine.schemaRegistry().register(person);
        engine.recordStore().create(person, new Record(null, person, Map.of("age", new LongValue(13))));
        engine.closeWithoutCheckpointForTest();

        engine.open(new StorageConfig(dbPath.toString(), 8192));
        assertNull(engine.root().get());
        assertNull(engine.schemaRegistry().entityType("Person"));
        assertFalse(engine.recordStore().scan(person).iterator().hasNext());
    }

    @Test
    public void testRecoveryReplaysCommittedDeleteAndUpdateFromPageWal() throws Exception {
        dbPath = Files.createTempFile("native-storage-page-wal", ".dbs");
        Files.deleteIfExists(dbPath);

        EntityType person = new EntityType(
                "Person",
                List.of(new FieldDefinition("age", ValueType.LONG, false, false)),
                List.of(new IndexDefinition("person_pk", IndexKind.PRIMARY, List.of("id")))
        );

        engine.open(new StorageConfig(dbPath.toString(), 8192));
        engine.schemaRegistry().register(person);

        Record keep;
        Record delete;
        try (Transaction tx = engine.begin(TransactionMode.READ_WRITE)) {
            keep = engine.recordStore().create(person, new Record(null, person, Map.of("age", new LongValue(20))));
            delete = engine.recordStore().create(person, new Record(null, person, Map.of("age", new LongValue(30))));
            tx.commit();
        }

        engine.begin(TransactionMode.READ_WRITE);
        engine.recordStore().update(new Record(keep.id(), person, Map.of("age", new LongValue(21))));
        engine.recordStore().delete(delete.id());
        engine.writeCommittedStateToWalForTest();
        engine.closeWithoutCheckpointForTest();

        engine.open(new StorageConfig(dbPath.toString(), 8192));
        assertEquals(new LongValue(21), engine.recordStore().get(keep.id()).fields().get("age"));
        assertNull(engine.recordStore().get(delete.id()));
    }

    @Test
    public void testDeleteReusesFreedRecordPage() throws Exception {
        dbPath = Files.createTempFile("native-storage-page-reuse", ".dbs");
        Files.deleteIfExists(dbPath);
        engine.open(new StorageConfig(dbPath.toString(), 8192));

        EntityType person = new EntityType(
                "Person",
                List.of(new FieldDefinition("age", ValueType.LONG, false, false)),
                List.of(new IndexDefinition("person_pk", IndexKind.PRIMARY, List.of("id")))
        );
        engine.schemaRegistry().register(person);

        PageBackedRecordStore recordStore = (PageBackedRecordStore) engine.recordStore();

        Record first;
        try (Transaction tx = engine.begin(TransactionMode.READ_WRITE)) {
            first = recordStore.create(person, new Record(null, person, Map.of("age", new LongValue(1))));
            tx.commit();
        }
        long sizeAfterFirstInsert = Files.size(recordStore.path());

        try (Transaction tx = engine.begin(TransactionMode.READ_WRITE)) {
            assertTrue(recordStore.delete(first.id()));
            tx.commit();
        }
        long sizeAfterDelete = Files.size(recordStore.path());

        try (Transaction tx = engine.begin(TransactionMode.READ_WRITE)) {
            recordStore.create(person, new Record(null, person, Map.of("age", new LongValue(2))));
            tx.commit();
        }
        long sizeAfterSecondInsert = Files.size(recordStore.path());

        assertEquals(sizeAfterFirstInsert, sizeAfterDelete);
        assertEquals(sizeAfterFirstInsert, sizeAfterSecondInsert);
    }

    @Test
    public void testAllocatorMetadataPersistsAcrossReopenForPageReuse() throws Exception {
        dbPath = Files.createTempFile("native-storage-allocator-reopen", ".dbs");
        Files.deleteIfExists(dbPath);
        engine.open(new StorageConfig(dbPath.toString(), 8192));

        EntityType person = new EntityType(
                "Person",
                List.of(new FieldDefinition("age", ValueType.LONG, false, false)),
                List.of(new IndexDefinition("person_pk", IndexKind.PRIMARY, List.of("id")))
        );
        engine.schemaRegistry().register(person);

        PageBackedRecordStore recordStore = (PageBackedRecordStore) engine.recordStore();

        Record first;
        try (Transaction tx = engine.begin(TransactionMode.READ_WRITE)) {
            first = recordStore.create(person, new Record(null, person, Map.of("age", new LongValue(5))));
            tx.commit();
        }
        long sizeAfterFirstInsert = Files.size(recordStore.path());

        try (Transaction tx = engine.begin(TransactionMode.READ_WRITE)) {
            assertTrue(recordStore.delete(first.id()));
            tx.commit();
        }
        engine.close();

        engine.open(new StorageConfig(dbPath.toString(), 8192));
        PageBackedRecordStore reopenedStore = (PageBackedRecordStore) engine.recordStore();
        long sizeAfterReopen = Files.size(reopenedStore.path());

        try (Transaction tx = engine.begin(TransactionMode.READ_WRITE)) {
            reopenedStore.create(person, new Record(null, person, Map.of("age", new LongValue(6))));
            tx.commit();
        }
        long sizeAfterSecondInsert = Files.size(reopenedStore.path());

        assertEquals(sizeAfterFirstInsert, sizeAfterReopen);
        assertEquals(sizeAfterFirstInsert, sizeAfterSecondInsert);
    }

    @Test
    public void testMultipleRecordsShareSingleAllocatedPage() throws Exception {
        dbPath = Files.createTempFile("native-storage-slotted-pages", ".dbs");
        Files.deleteIfExists(dbPath);
        engine.open(new StorageConfig(dbPath.toString(), 8192));

        EntityType person = new EntityType(
                "Person",
                List.of(new FieldDefinition("age", ValueType.LONG, false, false)),
                List.of(new IndexDefinition("person_pk", IndexKind.PRIMARY, List.of("id")))
        );
        engine.schemaRegistry().register(person);

        PageBackedRecordStore recordStore = (PageBackedRecordStore) engine.recordStore();

        try (Transaction tx = engine.begin(TransactionMode.READ_WRITE)) {
            for (int i = 0; i < 10; i++) {
                recordStore.create(person, new Record(null, person, Map.of("age", new LongValue(i))));
            }
            tx.commit();
        }

        long recordsFileSize = Files.size(recordStore.path());
        long expectedSinglePageSize = PageFile.HEADER_SIZE + 8192L;
        assertEquals(expectedSinglePageSize, recordsFileSize);
    }

    @Test
    public void testCommittedWalLogsOnlyDirtyRecordPages() throws Exception {
        dbPath = Files.createTempFile("native-storage-dirty-pages", ".dbs");
        Files.deleteIfExists(dbPath);
        engine.open(new StorageConfig(dbPath.toString(), 8192));

        EntityType person = new EntityType(
                "Person",
                List.of(new FieldDefinition("age", ValueType.LONG, false, false)),
                List.of(new IndexDefinition("person_pk", IndexKind.PRIMARY, List.of("id")))
        );
        engine.schemaRegistry().register(person);

        PageBackedRecordStore recordStore = (PageBackedRecordStore) engine.recordStore();
        Record first = null;
        try (Transaction tx = engine.begin(TransactionMode.READ_WRITE)) {
            for (int i = 0; i < 10_000 && recordStore.currentPageCount() < 2; i++) {
                Record created = recordStore.create(person, new Record(null, person, Map.of("age", new LongValue(i))));
                if (first == null) {
                    first = created;
                }
            }
            tx.commit();
        }

        assertNotNull(first);
        assertTrue(recordStore.currentPageCount() >= 2);

        long recordPageEntriesBefore = engine.writeAheadLog().readEntries().stream()
                .filter(entry -> entry.type() == WriteAheadLog.RECORD_PAGE_DIFF)
                .count();
        long deprecatedFullPageEntriesBefore = engine.writeAheadLog().readEntries().stream()
                .filter(entry -> entry.type() == DEPRECATED_RECORD_PAGE_ENTRY)
                .count();

        engine.begin(TransactionMode.READ_WRITE);
        recordStore.update(new Record(first.id(), person, Map.of("age", new LongValue(999))));
        engine.writeCommittedStateToWalForTest();

        long recordPageEntries = engine.writeAheadLog().readEntries().stream()
                .filter(entry -> entry.type() == WriteAheadLog.RECORD_PAGE_DIFF)
                .count();
        long deprecatedFullPageEntries = engine.writeAheadLog().readEntries().stream()
                .filter(entry -> entry.type() == DEPRECATED_RECORD_PAGE_ENTRY)
                .count();

        assertEquals(1L, recordPageEntries - recordPageEntriesBefore);
        assertEquals(0L, deprecatedFullPageEntries - deprecatedFullPageEntriesBefore);
        engine.closeWithoutCheckpointForTest();
    }

    @Test
    public void testCommittedWalSeparatesSlotIndexAndAllocatorMetadata() throws Exception {
        dbPath = Files.createTempFile("native-storage-split-metadata", ".dbs");
        Files.deleteIfExists(dbPath);
        engine.open(new StorageConfig(dbPath.toString(), 8192));

        EntityType person = new EntityType(
                "Person",
                List.of(new FieldDefinition("age", ValueType.LONG, false, false)),
                List.of(new IndexDefinition("person_pk", IndexKind.PRIMARY, List.of("id")))
        );
        engine.schemaRegistry().register(person);

        engine.begin(TransactionMode.READ_WRITE);
        engine.recordStore().create(person, new Record(null, person, Map.of("age", new LongValue(1))));
        engine.writeCommittedStateToWalForTest();

        long slotIndexEntries = engine.writeAheadLog().readEntries().stream()
                .filter(entry -> entry.type() == WriteAheadLog.RECORD_SLOT_INDEX)
                .count();
        long allocatorEntries = engine.writeAheadLog().readEntries().stream()
                .filter(entry -> entry.type() == WriteAheadLog.RECORD_ALLOCATOR_STATE)
                .count();
        long deprecatedCombinedEntries = engine.writeAheadLog().readEntries().stream()
                .filter(entry -> entry.type() == DEPRECATED_RECORD_STORE_METADATA_ENTRY)
                .count();

        assertEquals(1L, slotIndexEntries);
        assertEquals(1L, allocatorEntries);
        assertEquals(0L, deprecatedCombinedEntries);
        engine.closeWithoutCheckpointForTest();
    }

    @Test
    public void testPageDiffPayloadIsSmallerThanFullPageForSmallUpdate() throws Exception {
        dbPath = Files.createTempFile("native-storage-byte-diff", ".dbs");
        Files.deleteIfExists(dbPath);
        engine.open(new StorageConfig(dbPath.toString(), 8192));

        EntityType person = new EntityType(
                "Person",
                List.of(new FieldDefinition("age", ValueType.LONG, false, false)),
                List.of(new IndexDefinition("person_pk", IndexKind.PRIMARY, List.of("id")))
        );
        engine.schemaRegistry().register(person);

        PageBackedRecordStore recordStore = (PageBackedRecordStore) engine.recordStore();
        Record created;
        try (Transaction tx = engine.begin(TransactionMode.READ_WRITE)) {
            created = recordStore.create(person, new Record(null, person, Map.of("age", new LongValue(10))));
            tx.commit();
        }

        engine.begin(TransactionMode.READ_WRITE);
        recordStore.update(new Record(created.id(), person, Map.of("age", new LongValue(11))));

        byte[] diffPayload = recordStore.encodedDirtyPageDiffs().values().iterator().next();
        byte[] fullPagePayload = recordStore.encodedDirtyPages().values().iterator().next();

        assertTrue(diffPayload.length < fullPagePayload.length);
        engine.closeWithoutCheckpointForTest();
    }

    @Test
    public void testCorruptCommittedDiffBatchIsDiscardedDuringRecovery() throws Exception {
        dbPath = Files.createTempFile("native-storage-corrupt-diff", ".dbs");
        Files.deleteIfExists(dbPath);

        EntityType person = new EntityType(
                "Person",
                List.of(new FieldDefinition("age", ValueType.LONG, false, false)),
                List.of(new IndexDefinition("person_pk", IndexKind.PRIMARY, List.of("id")))
        );

        engine.open(new StorageConfig(dbPath.toString(), 8192));
        engine.schemaRegistry().register(person);

        Record created;
        try (Transaction tx = engine.begin(TransactionMode.READ_WRITE)) {
            created = engine.recordStore().create(person, new Record(null, person, Map.of("age", new LongValue(40))));
            tx.commit();
        }

        engine.begin(TransactionMode.READ_WRITE);
        engine.recordStore().update(new Record(created.id(), person, Map.of("age", new LongValue(41))));
        engine.writeCommittedStateToWalForTest();
        engine.corruptLastWalDiffByteForTest();
        engine.closeWithoutCheckpointForTest();

        engine.open(new StorageConfig(dbPath.toString(), 8192));
        assertEquals(new LongValue(40), engine.recordStore().get(created.id()).fields().get("age"));
    }

    @Test
    public void testMixedForeignBatchEntriesAreIgnoredDuringRecovery() throws Exception {
        dbPath = Files.createTempFile("native-storage-mixed-batch", ".dbs");
        Files.deleteIfExists(dbPath);

        EntityType person = new EntityType(
                "Person",
                List.of(new FieldDefinition("age", ValueType.LONG, false, false)),
                List.of(new IndexDefinition("person_pk", IndexKind.PRIMARY, List.of("id")))
        );

        engine.open(new StorageConfig(dbPath.toString(), 8192));
        engine.schemaRegistry().register(person);

        Record created;
        try (Transaction tx = engine.begin(TransactionMode.READ_WRITE)) {
            created = engine.recordStore().create(person, new Record(null, person, Map.of("age", new LongValue(70))));
            tx.commit();
        }

        engine.begin(TransactionMode.READ_WRITE);
        engine.recordStore().update(new Record(created.id(), person, Map.of("age", new LongValue(71))));
        engine.writeCommittedStateWithMixedForeignDiffForTest();
        engine.closeWithoutCheckpointForTest();

        engine.open(new StorageConfig(dbPath.toString(), 8192));
        assertEquals(new LongValue(71), engine.recordStore().get(created.id()).fields().get("age"));
    }

    @Test
    public void testCheckpointedBatchIsSkippedEvenIfLingeringWalIsCorrupt() throws Exception {
        dbPath = Files.createTempFile("native-storage-checkpoint-skip", ".dbs");
        Files.deleteIfExists(dbPath);

        EntityType person = new EntityType(
                "Person",
                List.of(new FieldDefinition("age", ValueType.LONG, false, false)),
                List.of(new IndexDefinition("person_pk", IndexKind.PRIMARY, List.of("id")))
        );

        engine.open(new StorageConfig(dbPath.toString(), 8192));
        engine.schemaRegistry().register(person);

        Record created;
        try (Transaction tx = engine.begin(TransactionMode.READ_WRITE)) {
            created = engine.recordStore().create(person, new Record(null, person, Map.of("age", new LongValue(90))));
            tx.commit();
        }

        engine.begin(TransactionMode.READ_WRITE);
        engine.recordStore().update(new Record(created.id(), person, Map.of("age", new LongValue(91))));
        engine.writeCommittedStateToWalForTest();
        engine.persistCheckpointWithoutClearingWalForTest();
        engine.corruptLastWalDiffByteForTest();
        engine.closeWithoutCheckpointForTest();

        engine.open(new StorageConfig(dbPath.toString(), 8192));
        assertEquals(new LongValue(91), engine.recordStore().get(created.id()).fields().get("age"));
    }

    @Test
    public void testCorruptLatestCheckpointFallsBackToPreviousSlotAndReplaysWal() throws Exception {
        dbPath = Files.createTempFile("native-storage-checkpoint-fallback", ".dbs");
        Files.deleteIfExists(dbPath);

        EntityType person = new EntityType(
                "Person",
                List.of(new FieldDefinition("age", ValueType.LONG, false, false)),
                List.of(new IndexDefinition("person_pk", IndexKind.PRIMARY, List.of("id")))
        );

        engine.open(new StorageConfig(dbPath.toString(), 8192));
        engine.schemaRegistry().register(person);

        Record created;
        try (Transaction tx = engine.begin(TransactionMode.READ_WRITE)) {
            created = engine.recordStore().create(person, new Record(null, person, Map.of("age", new LongValue(100))));
            tx.commit();
        }

        engine.begin(TransactionMode.READ_WRITE);
        engine.recordStore().update(new Record(created.id(), person, Map.of("age", new LongValue(101))));
        engine.writeCommittedStateToWalForTest();
        engine.persistCheckpointWithoutClearingWalForTest();
        engine.corruptLatestCheckpointByteForTest();
        engine.closeWithoutCheckpointForTest();

        engine.open(new StorageConfig(dbPath.toString(), 8192));
        assertEquals(new LongValue(101), engine.recordStore().get(created.id()).fields().get("age"));
    }

    @Test
    public void testRecoveryReplaysCommittedWalWhenLatestRecordStoreMetadataCheckpointIsCorrupt() throws Exception {
        dbPath = Files.createTempFile("native-storage-records-metadata-corrupt", ".dbs");
        Files.deleteIfExists(dbPath);

        EntityType person = new EntityType(
                "Person",
                List.of(new FieldDefinition("age", ValueType.LONG, false, false)),
                List.of(new IndexDefinition("person_pk", IndexKind.PRIMARY, List.of("id")))
        );

        engine.open(new StorageConfig(dbPath.toString(), 8192));
        engine.schemaRegistry().register(person);

        Record created;
        try (Transaction tx = engine.begin(TransactionMode.READ_WRITE)) {
            created = engine.recordStore().create(person, new Record(null, person, Map.of("age", new LongValue(110))));
            tx.commit();
        }

        engine.begin(TransactionMode.READ_WRITE);
        engine.recordStore().update(new Record(created.id(), person, Map.of("age", new LongValue(111))));
        engine.writeCommittedStateToWalForTest();
        engine.flushRecordStoreWithoutEngineCheckpointForTest();
        engine.corruptLatestRecordStoreMetadataByteForTest();
        engine.closeWithoutCheckpointForTest();

        engine.open(new StorageConfig(dbPath.toString(), 8192));
        assertEquals(new LongValue(111), engine.recordStore().get(created.id()).fields().get("age"));
    }

    @Test
    public void testRecoveryReplaysCommittedWalWhenLatestRecordStoreMetadataCheckpointIsIncomplete() throws Exception {
        dbPath = Files.createTempFile("native-storage-records-metadata-incomplete", ".dbs");
        Files.deleteIfExists(dbPath);

        EntityType person = new EntityType(
                "Person",
                List.of(new FieldDefinition("age", ValueType.LONG, false, false)),
                List.of(new IndexDefinition("person_pk", IndexKind.PRIMARY, List.of("id")))
        );

        engine.open(new StorageConfig(dbPath.toString(), 8192));
        engine.schemaRegistry().register(person);

        Record created;
        try (Transaction tx = engine.begin(TransactionMode.READ_WRITE)) {
            created = engine.recordStore().create(person, new Record(null, person, Map.of("age", new LongValue(120))));
            tx.commit();
        }

        engine.begin(TransactionMode.READ_WRITE);
        engine.recordStore().update(new Record(created.id(), person, Map.of("age", new LongValue(121))));
        engine.writeCommittedStateToWalForTest();
        engine.flushRecordStoreWithoutEngineCheckpointForTest();
        engine.writeIncompleteRecordStoreMetadataCheckpointForTest();
        engine.closeWithoutCheckpointForTest();

        engine.open(new StorageConfig(dbPath.toString(), 8192));
        assertEquals(new LongValue(121), engine.recordStore().get(created.id()).fields().get("age"));
    }

    @Test
    public void testWalReplayRestoresPersistedExactMatchIndexes() throws Exception {
        dbPath = Files.createTempFile("native-storage-index-replay", ".dbs");
        Files.deleteIfExists(dbPath);

        engine.open(new StorageConfig(dbPath.toString(), 8192));
        engine.indexManager().ensureNamespace("object:people", List.of("name"));
        engine.indexManager().add("object:people", Map.of("name", new StringValue("Ada")), "person-1");
        engine.close();

        engine.open(new StorageConfig(dbPath.toString(), 8192));
        try (Transaction tx = engine.begin(TransactionMode.READ_WRITE)) {
            engine.root().set("replayed-root");
            engine.writeCommittedStateToWalForTest();
        }
        engine.closeWithoutCheckpointForTest();

        engine.open(new StorageConfig(dbPath.toString(), 8192));
        assertTrue(engine.indexManager().hasField("object:people", "name"));
        assertEquals(java.util.Set.of("person-1"), engine.indexManager().find("object:people", "name", new StringValue("Ada")));
        assertEquals("replayed-root", engine.root().get(String.class));
    }

    @Test
    public void testCorruptIndexSidecarFallsBackToEmptyIndexManager() throws Exception {
        dbPath = Files.createTempFile("native-storage-index-corrupt", ".dbs");
        Files.deleteIfExists(dbPath);

        engine.open(new StorageConfig(dbPath.toString(), 8192));
        engine.indexManager().ensureNamespace("object:people", List.of("name"));
        engine.indexManager().add("object:people", Map.of("name", new StringValue("Ada")), "person-1");
        engine.close();

        PageFile indexFile = new PageFile(Path.of(dbPath + ".indexes"), 8192);
        try {
            indexFile.open();
            indexFile.corruptLatestPayloadByteForTest();
        } finally {
            indexFile.close();
        }

        engine.open(new StorageConfig(dbPath.toString(), 8192));
        assertFalse(engine.indexManager().hasField("object:people", "name"));
    }

    @Test
    public void testReopenPreservesPersistedOrderedRangeIndexes() throws Exception {
        dbPath = Files.createTempFile("native-storage-ordered-index", ".dbs");
        Files.deleteIfExists(dbPath);

        engine.open(new StorageConfig(dbPath.toString(), 8192));
        engine.orderedIndexManager().ensureNamespace("table:users", List.of("age"));
        engine.orderedIndexManager().add("table:users", "age", new LongValue(29L), "u1");
        engine.orderedIndexManager().add("table:users", "age", new LongValue(37L), "u2");
        engine.close();

        engine.open(new StorageConfig(dbPath.toString(), 8192));
        assertTrue(engine.orderedIndexManager().hasField("table:users", "age"));
        assertEquals(
                java.util.Set.of("u2"),
                engine.orderedIndexManager().range("table:users", "age", new LongValue(30L), new LongValue(40L))
        );
    }

    @Test
    public void testWalReplayRestoresPersistedOrderedRangeIndexes() throws Exception {
        dbPath = Files.createTempFile("native-storage-ordered-index-replay", ".dbs");
        Files.deleteIfExists(dbPath);

        engine.open(new StorageConfig(dbPath.toString(), 8192));
        engine.orderedIndexManager().ensureNamespace("table:users", List.of("age"));
        engine.orderedIndexManager().add("table:users", "age", new LongValue(37L), "u1");
        engine.close();

        engine.open(new StorageConfig(dbPath.toString(), 8192));
        try (Transaction tx = engine.begin(TransactionMode.READ_WRITE)) {
            engine.root().set("replayed-root");
            engine.writeCommittedStateToWalForTest();
        }
        engine.closeWithoutCheckpointForTest();

        engine.open(new StorageConfig(dbPath.toString(), 8192));
        assertTrue(engine.orderedIndexManager().hasField("table:users", "age"));
        assertEquals(
                java.util.Set.of("u1"),
                engine.orderedIndexManager().range("table:users", "age", new LongValue(30L), new LongValue(40L))
        );
        assertEquals("replayed-root", engine.root().get(String.class));
    }

    @Test
    public void testCorruptCombinedIndexSidecarFallsBackToEmptyManagers() throws Exception {
        dbPath = Files.createTempFile("native-storage-combined-index-corrupt", ".dbs");
        Files.deleteIfExists(dbPath);

        engine.open(new StorageConfig(dbPath.toString(), 8192));
        engine.indexManager().ensureNamespace("object:people", List.of("name"));
        engine.indexManager().add("object:people", Map.of("name", new StringValue("Ada")), "person-1");
        engine.orderedIndexManager().ensureNamespace("table:users", List.of("age"));
        engine.orderedIndexManager().add("table:users", "age", new LongValue(37L), "u1");
        engine.close();

        PageFile indexFile = new PageFile(Path.of(dbPath + ".indexes"), 8192);
        try {
            indexFile.open();
            indexFile.corruptLatestPayloadByteForTest();
        } finally {
            indexFile.close();
        }

        engine.open(new StorageConfig(dbPath.toString(), 8192));
        assertFalse(engine.indexManager().hasField("object:people", "name"));
        assertFalse(engine.orderedIndexManager().hasField("table:users", "age"));
    }

    @Test
    public void testIncrementalIndexWalReplaysWithoutIndexSnapshotSidecar() throws Exception {
        dbPath = Files.createTempFile("native-storage-index-wal-ops", ".dbs");
        Files.deleteIfExists(dbPath);

        engine.open(new StorageConfig(dbPath.toString(), 8192));
        try (Transaction tx = engine.begin(TransactionMode.READ_WRITE)) {
            engine.exactIndexAdd(
                    "object:people",
                    List.of("name"),
                    Map.of("name", new StringValue("Ada")),
                    "person-1"
            );
            engine.orderedIndexAdd(
                    "table:users",
                    List.of("age"),
                    "age",
                    new LongValue(37L),
                    "u1"
            );
            engine.writeCommittedStateToWalForTest();
        }
        engine.closeWithoutCheckpointForTest();

        WriteAheadLog wal = new WriteAheadLog(Path.of(dbPath + ".wal"));
        long exactNamespaceEntries;
        long exactAddEntries;
        long orderedNamespaceEntries;
        long orderedAddEntries;
        long deprecatedIndexSnapshotEntries;
        try {
            wal.open();
            exactNamespaceEntries = wal.readEntries().stream()
                    .filter(entry -> entry.type() == WriteAheadLog.EXACT_INDEX_NAMESPACE)
                    .count();
            exactAddEntries = wal.readEntries().stream()
                    .filter(entry -> entry.type() == WriteAheadLog.EXACT_INDEX_ADD)
                    .count();
            orderedNamespaceEntries = wal.readEntries().stream()
                    .filter(entry -> entry.type() == WriteAheadLog.ORDERED_INDEX_NAMESPACE)
                    .count();
            orderedAddEntries = wal.readEntries().stream()
                    .filter(entry -> entry.type() == WriteAheadLog.ORDERED_INDEX_ADD)
                    .count();
            deprecatedIndexSnapshotEntries = wal.readEntries().stream()
                    .filter(entry -> entry.type() == DEPRECATED_INDEX_SNAPSHOT_ENTRY)
                    .count();
        } finally {
            wal.close();
        }

        assertEquals(1L, exactNamespaceEntries);
        assertEquals(1L, exactAddEntries);
        assertEquals(1L, orderedNamespaceEntries);
        assertEquals(1L, orderedAddEntries);
        assertEquals(0L, deprecatedIndexSnapshotEntries);

        PageFile indexFile = new PageFile(Path.of(dbPath + ".indexes"), 8192);
        try {
            indexFile.open();
            indexFile.writePayload(new byte[0]);
        } finally {
            indexFile.close();
        }

        engine.open(new StorageConfig(dbPath.toString(), 8192));
        assertEquals(java.util.Set.of("person-1"), engine.indexManager().find("object:people", "name", new StringValue("Ada")));
        assertEquals(java.util.Set.of("u1"), engine.orderedIndexManager().range("table:users", "age", new LongValue(30L), new LongValue(40L)));
    }

    @Test
    public void testAutocommitIndexMutationsPersistAcrossReopen() throws Exception {
        dbPath = Files.createTempFile("native-storage-autocommit-index", ".dbs");
        Files.deleteIfExists(dbPath);

        engine.open(new StorageConfig(dbPath.toString(), 8192));
        engine.exactIndexAdd(
                "object:people",
                List.of("name", "city"),
                Map.of("name", new StringValue("Ada"), "city", new StringValue("Paris")),
                "person-1"
        );
        engine.exactIndexAdd(
                "object:people",
                List.of("name", "city"),
                Map.of("name", new StringValue("Grace"), "city", new StringValue("London")),
                "person-2"
        );
        engine.orderedIndexAdd(
                "table:users",
                List.of("age"),
                "age",
                new LongValue(37L),
                "u1"
        );
        engine.orderedIndexAdd(
                "table:users",
                List.of("age"),
                "age",
                new LongValue(41L),
                "u2"
        );
        engine.exactIndexRemove(
                "object:people",
                List.of("name", "city"),
                Map.of("name", new StringValue("Ada"), "city", new StringValue("Paris")),
                "person-1"
        );
        engine.orderedIndexRemove(
                "table:users",
                List.of("age"),
                "age",
                new LongValue(41L),
                "u2"
        );
        engine.close();

        engine.open(new StorageConfig(dbPath.toString(), 8192));
        assertEquals(java.util.Set.of(), engine.exactIndexFind("object:people", "name", new StringValue("Ada")));
        assertEquals(java.util.Set.of("person-2"), engine.exactIndexFind("object:people", "name", new StringValue("Grace")));
        assertEquals(java.util.Set.of("person-2"), engine.exactIndexFind("object:people", "city", new StringValue("London")));
        assertEquals(java.util.Set.of("u1"), engine.orderedIndexRange("table:users", "age", new LongValue(30L), new LongValue(40L)));
        assertEquals(java.util.Set.of(), engine.orderedIndexRange("table:users", "age", new LongValue(41L), new LongValue(41L)));
    }

    @Test
    public void testCommittedTransactionalIndexMutationsPersistWithoutFullCheckpoint() throws Exception {
        dbPath = Files.createTempFile("native-storage-transactional-index", ".dbs");
        Files.deleteIfExists(dbPath);

        engine.open(new StorageConfig(dbPath.toString(), 8192));
        try (Transaction tx = engine.begin(TransactionMode.READ_WRITE)) {
            engine.exactIndexAdd(
                    "object:people",
                    List.of("name", "city"),
                    Map.of("name", new StringValue("Ada"), "city", new StringValue("Paris")),
                    "person-1"
            );
            engine.orderedIndexAdd(
                    "table:users",
                    List.of("age"),
                    "age",
                    new LongValue(37L),
                    "u1"
            );
            tx.commit();
        }
        try (Transaction tx = engine.begin(TransactionMode.READ_WRITE)) {
            engine.exactIndexAdd(
                    "object:people",
                    List.of("name", "city"),
                    Map.of("name", new StringValue("Grace"), "city", new StringValue("London")),
                    "person-2"
            );
            engine.orderedIndexRemove(
                    "table:users",
                    List.of("age"),
                    "age",
                    new LongValue(37L),
                    "u1"
            );
            tx.commit();
        }

        engine.closeWithoutCheckpointForTest();

        engine.open(new StorageConfig(dbPath.toString(), 8192));
        assertEquals(java.util.Set.of("person-1"), engine.exactIndexFind("object:people", "name", new StringValue("Ada")));
        assertEquals(java.util.Set.of("person-2"), engine.exactIndexFind("object:people", "city", new StringValue("London")));
        assertEquals(java.util.Set.of(), engine.orderedIndexRange("table:users", "age", new LongValue(30L), new LongValue(40L)));
    }

    @Test
    public void testWalThresholdOnlyRequestsCheckpoint() throws Exception {
        dbPath = Files.createTempFile("native-storage-checkpoint-request", ".dbs");
        Files.deleteIfExists(dbPath);

        engine.open(new StorageConfig(dbPath.toString(), 8192, 1L, true));

        EntityType person = new EntityType(
                "Person",
                List.of(new FieldDefinition("age", ValueType.LONG, false, false)),
                List.of(new IndexDefinition("person_pk", IndexKind.PRIMARY, List.of("id")))
        );
        engine.schemaRegistry().register(person);

        try (Transaction tx = engine.begin(TransactionMode.READ_WRITE)) {
            engine.recordStore().create(person, new Record(null, person, Map.of("age", new LongValue(1L))));
            tx.commit();
        }

        assertTrue(engine.walSizeBytes() > 1L);
        assertTrue(engine.needsCheckpoint());
    }

    @Test
    public void testExplicitCheckpointClearsWalAndCheckpointRequest() throws Exception {
        dbPath = Files.createTempFile("native-storage-explicit-checkpoint", ".dbs");
        Files.deleteIfExists(dbPath);

        engine.open(new StorageConfig(dbPath.toString(), 8192, 1L, true));

        EntityType person = new EntityType(
                "Person",
                List.of(new FieldDefinition("age", ValueType.LONG, false, false)),
                List.of(new IndexDefinition("person_pk", IndexKind.PRIMARY, List.of("id")))
        );
        engine.schemaRegistry().register(person);

        try (Transaction tx = engine.begin(TransactionMode.READ_WRITE)) {
            engine.recordStore().create(person, new Record(null, person, Map.of("age", new LongValue(2L))));
            tx.commit();
        }

        assertTrue(engine.needsCheckpoint());
        assertTrue(engine.walSizeBytes() > 1L);

        engine.checkpoint();

        assertFalse(engine.needsCheckpoint());
        assertTrue(engine.walSizeBytes() <= 16L);
    }
}
