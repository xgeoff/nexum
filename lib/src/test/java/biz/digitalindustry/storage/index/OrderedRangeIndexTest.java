package biz.digitalindustry.storage.index;

import biz.digitalindustry.storage.engine.OrderedRangeIndexManager;
import biz.digitalindustry.storage.model.LongValue;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class OrderedRangeIndexTest {
    @Test
    public void testRangeLookupReturnsOrderedIdentifiers() {
        OrderedRangeIndex<String> index = new OrderedRangeIndex<>();
        index.add(new LongValue(20L), "u1");
        index.add(new LongValue(35L), "u2");
        index.add(new LongValue(40L), "u3");

        assertEquals(java.util.Set.of("u2", "u3"), index.range(new LongValue(30L), new LongValue(45L)));
    }

    @Test
    public void testManagerKeepsRangeNamespacesIsolated() {
        OrderedRangeIndexManager manager = new OrderedRangeIndexManager();
        manager.ensureNamespace("table:users", List.of("age"));
        manager.ensureNamespace("table:orders", List.of("total"));

        manager.add("table:users", "age", new LongValue(37L), "u1");
        manager.add("table:orders", "total", new LongValue(37L), "o1");

        assertEquals(java.util.Set.of("u1"), manager.range("table:users", "age", new LongValue(30L), new LongValue(40L)));
        assertEquals(java.util.Set.of("o1"), manager.range("table:orders", "total", new LongValue(30L), new LongValue(40L)));
    }

    @Test
    public void testManagerEncodeDecodeRoundTrip() {
        OrderedRangeIndexManager manager = new OrderedRangeIndexManager();
        manager.ensureNamespace("table:users", List.of("age"));
        manager.ensureNamespace("table:orders", List.of("total"));

        manager.add("table:users", "age", new LongValue(29L), "u1");
        manager.add("table:users", "age", new LongValue(37L), "u2");
        manager.add("table:orders", "total", new LongValue(105L), "o1");

        byte[] payload = manager.encode();

        OrderedRangeIndexManager restored = new OrderedRangeIndexManager();
        restored.decode(payload);

        assertEquals(java.util.Set.of("u2"), restored.range("table:users", "age", new LongValue(30L), new LongValue(40L)));
        assertEquals(java.util.Set.of("o1"), restored.range("table:orders", "total", new LongValue(100L), new LongValue(110L)));
    }
}
