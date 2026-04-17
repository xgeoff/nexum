package biz.digitalindustry.db.index;

import biz.digitalindustry.db.engine.ExactMatchIndexManager;
import biz.digitalindustry.db.model.LongValue;
import biz.digitalindustry.db.model.RecordId;
import biz.digitalindustry.db.model.StringValue;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ExactMatchIndexTest {
    @Test
    public void testAddFindAndRemoveEntries() {
        ExactMatchIndex<String> index = new ExactMatchIndex<>(List.of("name", "age"));

        index.add(java.util.Map.of(
                "name", new StringValue("Ada"),
                "age", new LongValue(37L)
        ), "u1");
        index.add(java.util.Map.of(
                "name", new StringValue("Ada"),
                "age", new LongValue(38L)
        ), "u2");

        assertEquals(java.util.Set.of("u1", "u2"), index.find("name", new StringValue("Ada")));
        assertEquals(java.util.Set.of("u1"), index.find("age", new LongValue(37L)));

        index.remove(java.util.Map.of(
                "name", new StringValue("Ada"),
                "age", new LongValue(37L)
        ), "u1");

        assertEquals(java.util.Set.of("u2"), index.find("name", new StringValue("Ada")));
        assertTrue(index.find("age", new LongValue(37L)).isEmpty());
    }

    @Test
    public void testManagerKeepsNamespacesIsolated() {
        ExactMatchIndexManager manager = new ExactMatchIndexManager();
        manager.ensureNamespace("object:people", List.of("name"));
        manager.ensureNamespace("table:users", List.of("name"));

        manager.add("object:people", java.util.Map.of("name", new StringValue("Ada")), "p1");
        manager.add("table:users", java.util.Map.of("name", new StringValue("Ada")), "u1");

        assertEquals(java.util.Set.of("p1"), manager.find("object:people", "name", new StringValue("Ada")));
        assertEquals(java.util.Set.of("u1"), manager.find("table:users", "name", new StringValue("Ada")));
    }

    @Test
    public void testManagerEncodeDecodeRoundTrip() {
        ExactMatchIndexManager manager = new ExactMatchIndexManager();
        manager.ensureNamespace("graph:nodes", List.of("id", "label"));
        manager.ensureNamespace("object:people", List.of("name"));

        manager.add("graph:nodes", java.util.Map.of(
                "id", new StringValue("alice"),
                "label", new StringValue("Person")
        ), new RecordId(7));
        manager.add("object:people", java.util.Map.of(
                "name", new StringValue("Ada")
        ), "person-1");

        byte[] payload = manager.encode();

        ExactMatchIndexManager restored = new ExactMatchIndexManager();
        restored.decode(payload);

        assertEquals(java.util.Set.of(new RecordId(7)), restored.find("graph:nodes", "id", new StringValue("alice")));
        assertEquals(java.util.Set.of("person-1"), restored.find("object:people", "name", new StringValue("Ada")));
    }
}
