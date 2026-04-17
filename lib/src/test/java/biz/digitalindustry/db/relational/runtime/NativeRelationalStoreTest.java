package biz.digitalindustry.db.relational.runtime;

import biz.digitalindustry.db.engine.NativeStorageEngine;
import biz.digitalindustry.db.model.Record;
import biz.digitalindustry.db.relational.api.RelationalStore;
import biz.digitalindustry.db.relational.api.Row;
import biz.digitalindustry.db.relational.api.TableDefinition;
import biz.digitalindustry.db.model.BooleanValue;
import biz.digitalindustry.db.model.LongValue;
import biz.digitalindustry.db.model.StringValue;
import biz.digitalindustry.db.model.Vector;
import biz.digitalindustry.db.model.VectorValue;
import biz.digitalindustry.db.query.QueryCommand;
import biz.digitalindustry.db.query.QueryResult;
import biz.digitalindustry.db.query.vector.VectorQueryProvider;
import biz.digitalindustry.db.schema.FieldDefinition;
import biz.digitalindustry.db.schema.IndexDefinition;
import biz.digitalindustry.db.schema.IndexKind;
import biz.digitalindustry.db.schema.ValueType;
import biz.digitalindustry.db.relational.api.VectorRowMatch;
import biz.digitalindustry.db.vector.api.VectorCollectionDefinition;
import biz.digitalindustry.db.vector.api.VectorDocument;
import biz.digitalindustry.db.vector.runtime.NativeVectorStore;
import org.junit.After;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class NativeRelationalStoreTest {
    private static final TableDefinition USER_TABLE = new TableDefinition(
            "users",
            "id",
            List.of(
                    new FieldDefinition("id", ValueType.STRING, true, false),
                    new FieldDefinition("name", ValueType.STRING, true, false),
                    new FieldDefinition("age", ValueType.LONG, true, false),
                    new FieldDefinition("active", ValueType.BOOLEAN, true, false)
            ),
            List.of(
                    new IndexDefinition("users_name_idx", IndexKind.NON_UNIQUE, List.of("name")),
                    new IndexDefinition("users_active_idx", IndexKind.NON_UNIQUE, List.of("active")),
                    new IndexDefinition("users_age_range_idx", IndexKind.ORDERED_RANGE, List.of("age"))
            )
    );
    private static final TableDefinition EMBEDDING_TABLE = new TableDefinition(
            "embeddings",
            "id",
            List.of(
                    new FieldDefinition("id", ValueType.STRING, true, false),
                    FieldDefinition.vector("embedding", 3, true),
                    new FieldDefinition("label", ValueType.STRING, true, false)
            ),
            List.of(
                    IndexDefinition.vector("embeddings_embedding_idx", "embedding", 3, "euclidean")
            )
    );

    private NativeRelationalStore store;
    private NativeVectorStore vectorStore;
    private Path dbPath;

    @After
    public void tearDown() throws Exception {
        if (store != null) {
            store.close();
            store = null;
        }
        if (vectorStore != null) {
            vectorStore.close();
            vectorStore = null;
        }
        if (dbPath != null) {
            Files.deleteIfExists(Path.of(dbPath + ".records"));
            Files.deleteIfExists(Path.of(dbPath + ".wal"));
            Files.deleteIfExists(Path.of(dbPath + ".indexes"));
            Files.deleteIfExists(dbPath);
        }
    }

    @Test
    public void testUpsertGetAndGetAllRows() throws Exception {
        openStore("native-relational-basic");

        Row inserted = store.upsert(USER_TABLE, userRow("u1", "Ada", 37L, true));
        assertEquals("u1", inserted.primaryKey());

        Row loaded = store.get(USER_TABLE, "u1");
        assertNotNull(loaded);
        assertEquals(new StringValue("Ada"), loaded.values().get("name"));
        assertEquals(new LongValue(37L), loaded.values().get("age"));

        List<Row> rows = store.getAll(USER_TABLE);
        assertEquals(1, rows.size());
        assertEquals("u1", rows.get(0).primaryKey());
    }

    @Test
    public void testUpsertUpdatesExistingPrimaryKey() throws Exception {
        openStore("native-relational-update");

        store.upsert(USER_TABLE, userRow("u1", "Ada", 37L, true));
        store.upsert(USER_TABLE, userRow("u1", "Ada Lovelace", 38L, true));

        Row loaded = store.get(USER_TABLE, "u1");
        assertEquals(new StringValue("Ada Lovelace"), loaded.values().get("name"));
        assertEquals(new LongValue(38L), loaded.values().get("age"));
    }

    @Test
    public void testFindRowsByExactColumnValue() throws Exception {
        openStore("native-relational-find");

        store.upsert(USER_TABLE, userRow("u1", "Ada", 37L, true));
        store.upsert(USER_TABLE, userRow("u2", "Grace", 37L, false));
        store.upsert(USER_TABLE, userRow("u3", "Linus", 45L, true));

        List<Row> ageMatches = store.findBy(USER_TABLE, "age", new LongValue(37L));
        assertEquals(2, ageMatches.size());
        assertEquals("u1", ageMatches.get(0).primaryKey());
        assertEquals("u2", ageMatches.get(1).primaryKey());

        Row activeMatch = store.findOneBy(USER_TABLE, "active", new BooleanValue(true));
        assertNotNull(activeMatch);
        assertEquals("u1", activeMatch.primaryKey());
    }

    @Test
    public void testDeleteByPrimaryKey() throws Exception {
        openStore("native-relational-delete");

        store.upsert(USER_TABLE, userRow("u1", "Ada", 37L, true));

        assertTrue(store.delete(USER_TABLE, "u1"));
        assertNull(store.get(USER_TABLE, "u1"));
        assertFalse(store.delete(USER_TABLE, "u1"));
    }

    @Test
    public void testFindRowsByRange() throws Exception {
        openStore("native-relational-range");

        store.upsert(USER_TABLE, userRow("u1", "Ada", 29L, true));
        store.upsert(USER_TABLE, userRow("u2", "Grace", 37L, false));
        store.upsert(USER_TABLE, userRow("u3", "Linus", 45L, true));

        List<Row> matches = store.findRangeBy(USER_TABLE, "age", new LongValue(30L), new LongValue(40L));
        assertEquals(1, matches.size());
        assertEquals("u2", matches.get(0).primaryKey());
    }

    @Test
    public void testReopenPreservesRows() throws Exception {
        openStore("native-relational-reopen");

        store.upsert(USER_TABLE, userRow("u1", "Ada", 37L, true));
        store.close();
        store = null;

        store = new NativeRelationalStore(dbPath.toString());
        store.registerTable(USER_TABLE);

        Row loaded = store.get(USER_TABLE, "u1");
        assertNotNull(loaded);
        assertEquals(new StringValue("Ada"), loaded.values().get("name"));
        assertEquals(new BooleanValue(true), loaded.values().get("active"));
    }

    @Test
    public void testReopenRestoresRegisteredTableDefinition() throws Exception {
        openStore("native-relational-schema-reopen");

        store.upsert(USER_TABLE, userRow("u1", "Ada", 37L, true));
        store.close();
        store = null;

        store = new NativeRelationalStore(dbPath.toString());

        TableDefinition reopened = store.table("users");
        assertNotNull(reopened);
        assertEquals("id", reopened.primaryKeyColumn());
        assertEquals(4, reopened.columns().size());

        Row loaded = store.get(reopened, "u1");
        assertNotNull(loaded);
        assertEquals(new StringValue("Ada"), loaded.values().get("name"));
    }

    @Test
    public void testFindByReflectsUpdatesDeletesAndReopen() throws Exception {
        openStore("native-relational-index-rebuild");

        store.upsert(USER_TABLE, userRow("u1", "Ada", 37L, true));
        store.upsert(USER_TABLE, userRow("u2", "Grace", 37L, false));

        store.upsert(USER_TABLE, userRow("u2", "Grace Hopper", 38L, true));
        store.delete(USER_TABLE, "u1");

        assertEquals(0, store.findBy(USER_TABLE, "age", new LongValue(37L)).size());
        List<Row> updated = store.findBy(USER_TABLE, "name", new StringValue("Grace Hopper"));
        assertEquals(1, updated.size());
        assertEquals("u2", updated.get(0).primaryKey());

        store.close();
        store = null;

        store = new NativeRelationalStore(dbPath.toString());
        store.registerTable(USER_TABLE);

        List<Row> reopened = store.findBy(USER_TABLE, "name", new StringValue("Grace Hopper"));
        assertEquals(1, reopened.size());
        assertEquals("u2", reopened.get(0).primaryKey());
    }

    @Test
    public void testReaderDuringWriteSeesLastCommittedRowState() throws Exception {
        openStore("native-relational-overlap");

        store.upsert(USER_TABLE, userRow("u1", "Ada", 37L, true));
        NativeStorageEngine engine = underlyingEngine(store);

        CountDownLatch readerDone = new CountDownLatch(1);
        AtomicReference<Row> observed = new AtomicReference<>();

        try (var tx = engine.begin(biz.digitalindustry.db.engine.tx.TransactionMode.READ_WRITE)) {
            var recordId = (biz.digitalindustry.db.model.RecordId) engine
                    .exactIndexFind("table:users", TableDefinition.PRIMARY_KEY_FIELD, new StringValue("u1"))
                    .iterator()
                    .next();
            Record current = engine.recordStore().get(recordId);
            engine.recordStore().update(new Record(
                    current.id(),
                    current.type(),
                    Map.of(
                            "rowKey", new StringValue("u1"),
                            "id", new StringValue("u1"),
                            "name", new StringValue("Ada Lovelace"),
                            "age", new LongValue(37L),
                            "active", new BooleanValue(true)
                    )
            ));

            Thread reader = new Thread(() -> {
                try {
                    observed.set(store.get(USER_TABLE, "u1"));
                } finally {
                    readerDone.countDown();
                }
            });
            reader.start();

            assertTrue(readerDone.await(2, TimeUnit.SECONDS));
            assertEquals(new StringValue("Ada"), observed.get().values().get("name"));
            tx.commit();
        }

        assertEquals(new StringValue("Ada Lovelace"), store.get(USER_TABLE, "u1").values().get("name"));
    }

    @Test
    public void testFindNearestByVectorColumn() throws Exception {
        openStore("native-relational-vector");
        store.registerTable(EMBEDDING_TABLE);

        store.upsert(EMBEDDING_TABLE, embeddingRow("e1", "alpha", Vector.of(1.0f, 0.0f, 0.0f)));
        store.upsert(EMBEDDING_TABLE, embeddingRow("e2", "beta", Vector.of(0.9f, 0.1f, 0.0f)));
        store.upsert(EMBEDDING_TABLE, embeddingRow("e3", "gamma", Vector.of(0.0f, 1.0f, 0.0f)));

        List<VectorRowMatch> matches = store.findNearestBy(EMBEDDING_TABLE, "embedding", Vector.of(1.0f, 0.0f, 0.0f), 2);

        assertEquals(2, matches.size());
        assertEquals("e1", matches.get(0).row().primaryKey());
        assertEquals("e2", matches.get(1).row().primaryKey());
    }

    @Test
    public void testVectorColumnRejectsWrongDimension() throws Exception {
        openStore("native-relational-vector-dimension");
        store.registerTable(EMBEDDING_TABLE);

        try {
            store.upsert(EMBEDDING_TABLE, embeddingRow("e1", "alpha", Vector.of(1.0f, 0.0f)));
        } catch (IllegalArgumentException ex) {
            assertTrue(ex.getMessage().contains("dimension 3"));
            return;
        }
        throw new AssertionError("Expected vector dimension validation to fail");
    }

    @Test
    public void testVectorQueryProviderExecutesEndToEnd() throws Exception {
        openVectorStore("native-vector-provider");
        VectorCollectionDefinition collection = embeddingCollection();
        vectorStore.registerCollection(collection);
        vectorStore.upsert(collection, embeddingDocument("e1", "alpha", Vector.of(1.0f, 0.0f, 0.0f)));
        vectorStore.upsert(collection, embeddingDocument("e2", "beta", Vector.of(0.0f, 1.0f, 0.0f)));

        VectorQueryProvider provider = new VectorQueryProvider(vectorStore);
        QueryResult result = provider.execute(new QueryCommand("vector", Map.of(
                "from", "embeddings",
                "vector", Map.of(
                        "field", "embedding",
                        "nearest", Map.of(
                                "vector", List.of(1.0, 0.0, 0.0),
                                "k", 1,
                                "distance", "euclidean"
                        )
                )
        )));

        assertEquals(1, result.rows().size());
        assertEquals("e1", result.rows().get(0).get("result").id());
        assertEquals("vectorDocument", result.rows().get(0).get("result").properties().get("entityType"));
    }

    @Test
    public void testVectorQueryProviderSupportsVectorText() throws Exception {
        openVectorStore("native-vector-text-provider");
        VectorCollectionDefinition collection = embeddingCollection();
        vectorStore.registerCollection(collection);
        vectorStore.upsert(collection, embeddingDocument("e1", "alpha", Vector.of(1.0f, 0.0f, 0.0f)));
        vectorStore.upsert(collection, embeddingDocument("e2", "beta", Vector.of(0.9f, 0.1f, 0.0f)));

        VectorQueryProvider provider = new VectorQueryProvider(vectorStore);
        QueryResult result = provider.execute(new QueryCommand("vector", Map.of(
                "queryText", """
                        VECTOR FROM embeddings
                        FIELD embedding
                        NEAREST [1.0, 0.0, 0.0]
                        K 2
                        DISTANCE euclidean
                        """
        )));

        assertEquals(2, result.rows().size());
        assertEquals("e1", result.rows().get(0).get("result").id());
        assertEquals("e2", result.rows().get(1).get("result").id());
    }

    private void openStore(String prefix) throws Exception {
        dbPath = Files.createTempFile(prefix, ".dbs");
        Files.deleteIfExists(dbPath);
        store = new NativeRelationalStore(dbPath.toString());
        store.registerTable(USER_TABLE);
    }

    private void openVectorStore(String prefix) throws Exception {
        dbPath = Files.createTempFile(prefix, ".dbs");
        Files.deleteIfExists(dbPath);
        vectorStore = new NativeVectorStore(dbPath.toString());
    }

    private Row userRow(String id, String name, long age, boolean active) {
        return new Row(id, Map.of(
                "id", new StringValue(id),
                "name", new StringValue(name),
                "age", new LongValue(age),
                "active", new BooleanValue(active)
        ));
    }

    private Row embeddingRow(String id, String label, Vector embedding) {
        return new Row(id, Map.of(
                "id", new StringValue(id),
                "label", new StringValue(label),
                "embedding", new VectorValue(embedding)
        ));
    }

    private VectorCollectionDefinition embeddingCollection() {
        return new VectorCollectionDefinition(
                "embeddings",
                "id",
                "embedding",
                3,
                "euclidean",
                List.of(
                        new FieldDefinition("id", ValueType.STRING, true, false),
                        FieldDefinition.vector("embedding", 3, true),
                        new FieldDefinition("label", ValueType.STRING, true, false)
                ),
                List.of()
        );
    }

    private VectorDocument embeddingDocument(String id, String label, Vector embedding) {
        return new VectorDocument(id, Map.of(
                "id", new StringValue(id),
                "label", new StringValue(label),
                "embedding", new VectorValue(embedding)
        ));
    }

    private NativeStorageEngine underlyingEngine(NativeRelationalStore store) throws Exception {
        var field = NativeRelationalStore.class.getDeclaredField("storageEngine");
        field.setAccessible(true);
        return (NativeStorageEngine) field.get(store);
    }
}
