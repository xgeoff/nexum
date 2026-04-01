package biz.digitalindustry.relational.engine;

import biz.digitalindustry.relational.api.RelationalStore;
import biz.digitalindustry.relational.api.Row;
import biz.digitalindustry.relational.api.TableDefinition;
import biz.digitalindustry.storage.model.BooleanValue;
import biz.digitalindustry.storage.model.LongValue;
import biz.digitalindustry.storage.model.StringValue;
import biz.digitalindustry.storage.schema.FieldDefinition;
import biz.digitalindustry.storage.schema.IndexDefinition;
import biz.digitalindustry.storage.schema.IndexKind;
import biz.digitalindustry.storage.schema.ValueType;
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

    private NativeRelationalStore store;
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

    private void openStore(String prefix) throws Exception {
        dbPath = Files.createTempFile(prefix, ".dbs");
        Files.deleteIfExists(dbPath);
        store = new NativeRelationalStore(dbPath.toString());
        store.registerTable(USER_TABLE);
    }

    private Row userRow(String id, String name, long age, boolean active) {
        return new Row(id, Map.of(
                "id", new StringValue(id),
                "name", new StringValue(name),
                "age", new LongValue(age),
                "active", new BooleanValue(active)
        ));
    }
}
