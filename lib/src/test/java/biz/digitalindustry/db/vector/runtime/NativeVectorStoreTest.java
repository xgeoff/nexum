package biz.digitalindustry.db.vector.runtime;

import biz.digitalindustry.db.model.LongValue;
import biz.digitalindustry.db.model.StringValue;
import biz.digitalindustry.db.model.Vector;
import biz.digitalindustry.db.model.VectorValue;
import biz.digitalindustry.db.schema.FieldDefinition;
import biz.digitalindustry.db.schema.ValueType;
import biz.digitalindustry.db.vector.api.VectorCollectionDefinition;
import biz.digitalindustry.db.vector.api.VectorDocument;
import biz.digitalindustry.db.vector.api.VectorDocumentMatch;
import org.junit.After;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class NativeVectorStoreTest {
    private static final VectorCollectionDefinition EMBEDDINGS = new VectorCollectionDefinition(
            "embeddings",
            "id",
            "embedding",
            3,
            "cosine",
            List.of(
                    new FieldDefinition("id", ValueType.STRING, true, false),
                    FieldDefinition.vector("embedding", 3, true),
                    new FieldDefinition("label", ValueType.STRING, true, false),
                    new FieldDefinition("version", ValueType.LONG, false, false)
            ),
            List.of()
    );

    private NativeVectorStore store;
    private Path dbPath;

    @After
    public void tearDown() throws Exception {
        if (store != null) {
            store.close();
            store = null;
        }
        if (dbPath != null) {
            Files.deleteIfExists(Path.of(dbPath + ".records"));
            Files.deleteIfExists(Path.of(dbPath + ".wal"));
            Files.deleteIfExists(dbPath);
        }
    }

    @Test
    public void testUpsertGetAndNearest() throws Exception {
        openStore("native-vector-basic");

        store.upsert(EMBEDDINGS, document("e1", "alpha", Vector.of(1.0f, 0.0f, 0.0f), 1L));
        store.upsert(EMBEDDINGS, document("e2", "beta", Vector.of(0.0f, 1.0f, 0.0f), 2L));
        store.upsert(EMBEDDINGS, document("e3", "gamma", Vector.of(0.9f, 0.1f, 0.0f), 3L));

        VectorDocument loaded = store.get(EMBEDDINGS, "e1");
        assertNotNull(loaded);
        assertEquals(new StringValue("alpha"), loaded.values().get("label"));

        List<VectorDocumentMatch> matches = store.nearest(EMBEDDINGS, Vector.of(1.0f, 0.0f, 0.0f), 2);
        assertEquals(2, matches.size());
        assertEquals("e1", matches.get(0).document().key());
        assertEquals("e3", matches.get(1).document().key());
    }

    @Test
    public void testReopenRestoresCollectionDefinition() throws Exception {
        openStore("native-vector-reopen");

        store.upsert(EMBEDDINGS, document("e1", "alpha", Vector.of(1.0f, 0.0f, 0.0f), 1L));
        store.close();
        store = null;

        store = new NativeVectorStore(dbPath.toString());

        VectorCollectionDefinition reopened = store.collection("embeddings");
        assertNotNull(reopened);
        assertEquals("id", reopened.keyField());
        assertEquals("embedding", reopened.vectorField());
        assertEquals(3, reopened.dimension());

        VectorDocument loaded = store.get(reopened, "e1");
        assertNotNull(loaded);
        assertEquals(new StringValue("alpha"), loaded.values().get("label"));
    }

    @Test
    public void testDeleteRemovesVectorDocument() throws Exception {
        openStore("native-vector-delete");

        store.upsert(EMBEDDINGS, document("e1", "alpha", Vector.of(1.0f, 0.0f, 0.0f), 1L));

        assertTrue(store.delete(EMBEDDINGS, "e1"));
        assertNull(store.get(EMBEDDINGS, "e1"));
        assertFalse(store.delete(EMBEDDINGS, "e1"));
    }

    private void openStore(String name) throws Exception {
        dbPath = Files.createTempDirectory(name).resolveSibling(name + ".dbs");
        Files.deleteIfExists(dbPath);
        store = new NativeVectorStore(dbPath.toString());
        store.registerCollection(EMBEDDINGS);
    }

    private static VectorDocument document(String id, String label, Vector embedding, long version) {
        return new VectorDocument(id, Map.of(
                "id", new StringValue(id),
                "label", new StringValue(label),
                "embedding", new VectorValue(embedding),
                "version", new LongValue(version)
        ));
    }
}
