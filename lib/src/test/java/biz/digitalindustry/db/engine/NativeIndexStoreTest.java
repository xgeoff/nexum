package biz.digitalindustry.db.engine;

import biz.digitalindustry.db.model.LongValue;
import biz.digitalindustry.db.model.StringValue;
import biz.digitalindustry.db.engine.page.PageFile;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class NativeIndexStoreTest {
    @Test
    public void testPersistAndLoadNamespacesFromPageBackedIndexStore() throws Exception {
        Path dbPath = Files.createTempFile("native-index-store", ".idx");
        Files.deleteIfExists(dbPath);

        ExactMatchIndexManager exact = new ExactMatchIndexManager();
        exact.ensureNamespace("object:people", List.of("name", "city"));
        exact.add("object:people", java.util.Map.of(
                "name", new StringValue("Ada"),
                "city", new StringValue("Paris")
        ), "person-1");
        exact.add("object:people", java.util.Map.of(
                "name", new StringValue("Grace"),
                "city", new StringValue("London")
        ), "person-2");

        OrderedRangeIndexManager ordered = new OrderedRangeIndexManager();
        ordered.ensureNamespace("table:users", List.of("age", "score"));
        ordered.add("table:users", "age", new LongValue(37L), "u1");
        ordered.add("table:users", "age", new LongValue(41L), "u2");
        ordered.add("table:users", "age", new LongValue(35L), "u3");
        ordered.add("table:users", "score", new LongValue(88L), "u1");
        ordered.add("table:users", "score", new LongValue(91L), "u2");
        ordered.add("table:users", "score", new LongValue(84L), "u3");

        NativeIndexStore store = new NativeIndexStore(dbPath, 8192);
        try {
            store.open();
            store.persist(exact, ordered);
            assertTrue(Files.size(store.path()) > 4096L);
        } finally {
            store.close();
        }

        PageFile pageFile = new PageFile(dbPath, 8192);
        try {
            pageFile.open();
            assertTrue(pageFile.getPageCount() >= 8);
        } finally {
            pageFile.close();
        }

        ExactMatchIndexManager restoredExact = new ExactMatchIndexManager();
        OrderedRangeIndexManager restoredOrdered = new OrderedRangeIndexManager();
        store = new NativeIndexStore(dbPath, 8192);
        try {
            store.open();
            assertEquals(java.util.Set.of("person-1"), store.findExact((byte) 1, "object:people", "name", new StringValue("Ada")));
            assertEquals(java.util.Set.of("u1", "u3"), store.findRange("table:users", "age", new LongValue(30L), new LongValue(40L)));
            assertEquals(java.util.Set.of("u2"), store.findRange("table:users", "score", new LongValue(90L), new LongValue(95L)));
            store.loadInto(restoredExact, restoredOrdered);
        } finally {
            store.close();
            Files.deleteIfExists(dbPath);
        }

        assertEquals(java.util.Set.of("person-1"), restoredExact.find("object:people", "name", new StringValue("Ada")));
        assertEquals(java.util.Set.of("person-1"), restoredExact.find("object:people", "city", new StringValue("Paris")));
        assertEquals(java.util.Set.of("person-2"), restoredExact.find("object:people", "name", new StringValue("Grace")));
        assertEquals(java.util.Set.of("person-2"), restoredExact.find("object:people", "city", new StringValue("London")));
        assertEquals(java.util.Set.of("u1", "u3"), restoredOrdered.range("table:users", "age", new LongValue(30L), new LongValue(40L)));
        assertEquals(java.util.Set.of("u1", "u3"), restoredOrdered.range("table:users", "score", new LongValue(80L), new LongValue(90L)));
        assertEquals(java.util.Set.of("u2"), restoredOrdered.range("table:users", "age", new LongValue(40L), new LongValue(45L)));
        assertEquals(java.util.Set.of("u2"), restoredOrdered.range("table:users", "score", new LongValue(90L), new LongValue(95L)));
    }

    @Test
    public void testPagedFieldDirectoryLookupAcrossMultipleDirectoryPages() throws Exception {
        Path dbPath = Files.createTempFile("native-index-store-paged-directory", ".idx");
        Files.deleteIfExists(dbPath);

        OrderedRangeIndexManager ordered = new OrderedRangeIndexManager();
        ordered.ensureNamespace("table:users", List.of("age"));
        for (long age = 10; age < 40; age++) {
            ordered.add("table:users", "age", new LongValue(age), "u" + age);
        }

        NativeIndexStore store = new NativeIndexStore(dbPath, 256);
        try {
            store.open();
            store.persist(new ExactMatchIndexManager(), ordered);

            PageFile pageFile = new PageFile(dbPath, 256);
            try {
                pageFile.open();
                assertTrue(pageFile.getPageCount() > 5);
            } finally {
                pageFile.close();
            }

            assertEquals(java.util.Set.of("u12"), store.findRange("table:users", "age", new LongValue(12L), new LongValue(12L)));
            assertEquals(java.util.Set.of("u28", "u29", "u30", "u31", "u32"), store.findRange("table:users", "age", new LongValue(28L), new LongValue(32L)));
        } finally {
            store.close();
            Files.deleteIfExists(dbPath);
        }
    }

    @Test
    public void testPagedFieldDirectoryLookupAcrossMultipleSummaryPages() throws Exception {
        Path dbPath = Files.createTempFile("native-index-store-multi-summary", ".idx");
        Files.deleteIfExists(dbPath);

        OrderedRangeIndexManager ordered = new OrderedRangeIndexManager();
        ordered.ensureNamespace("table:users", List.of("age"));
        for (long age = 100; age < 220; age++) {
            ordered.add("table:users", "age", new LongValue(age), "u" + age);
        }

        NativeIndexStore store = new NativeIndexStore(dbPath, 192);
        try {
            store.open();
            store.persist(new ExactMatchIndexManager(), ordered);

            PageFile pageFile = new PageFile(dbPath, 192);
            try {
                pageFile.open();
                assertTrue(pageFile.getPageCount() > 20);
            } finally {
                pageFile.close();
            }

            assertEquals(java.util.Set.of("u105"), store.findRange("table:users", "age", new LongValue(105L), new LongValue(105L)));
            assertEquals(
                    java.util.Set.of("u176", "u177", "u178", "u179", "u180", "u181", "u182"),
                    store.findRange("table:users", "age", new LongValue(176L), new LongValue(182L))
            );
        } finally {
            store.close();
            Files.deleteIfExists(dbPath);
        }
    }

    @Test
    public void testPagedFieldDirectoryLookupAcrossMultipleInternalLevels() throws Exception {
        Path dbPath = Files.createTempFile("native-index-store-multi-level-tree", ".idx");
        Files.deleteIfExists(dbPath);

        OrderedRangeIndexManager ordered = new OrderedRangeIndexManager();
        ordered.ensureNamespace("table:users", List.of("age"));
        for (long age = 1000; age < 1600; age++) {
            ordered.add("table:users", "age", new LongValue(age), "u" + age);
        }

        NativeIndexStore store = new NativeIndexStore(dbPath, 160);
        try {
            store.open();
            store.persist(new ExactMatchIndexManager(), ordered);

            NativeIndexStore.DirectoryTreeMetrics metrics = store.describeTree((byte) 2, "table:users", "age");

            PageFile pageFile = new PageFile(dbPath, 160);
            try {
                pageFile.open();
                assertTrue(pageFile.getPageCount() > 80);
            } finally {
                pageFile.close();
            }

            assertEquals(java.util.Set.of("u1003"), store.findRange("table:users", "age", new LongValue(1003L), new LongValue(1003L)));
            assertEquals(
                    java.util.Set.of("u1450", "u1451", "u1452", "u1453", "u1454", "u1455"),
                    store.findRange("table:users", "age", new LongValue(1450L), new LongValue(1455L))
            );
            assertTrue(metrics.height() >= 3);
            assertTrue(metrics.internalPageCount() > 1);
            assertTrue(metrics.leafPageCount() > 1);
            assertTrue(metrics.minLeafEntries() > 1);
            assertTrue(metrics.minInternalEntries() > 1);
            assertTrue(metrics.maxLeafEntries() - metrics.minLeafEntries() <= 1);
        } finally {
            store.close();
            Files.deleteIfExists(dbPath);
        }
    }

    @Test
    public void testExistingValueMutationKeepsTreeShapeStable() throws Exception {
        Path dbPath = Files.createTempFile("native-index-store-in-place-leaf-update", ".idx");
        Files.deleteIfExists(dbPath);

        ExactMatchIndexManager exact = new ExactMatchIndexManager();
        exact.ensureNamespace("object:people", List.of("name"));
        for (int i = 0; i < 40; i++) {
            exact.add("object:people", java.util.Map.of("name", new StringValue("name-" + i)), "person-" + i);
        }

        NativeIndexStore store = new NativeIndexStore(dbPath, 192);
        try {
            store.open();
            store.persist(exact, new OrderedRangeIndexManager());

            NativeIndexStore.DirectoryTreeMetrics before = store.describeTree((byte) 1, "object:people", "name");
            store.applyExactAdd("object:people", "name", new StringValue("name-12"), "person-12b");
            store.applyExactRemove("object:people", "name", new StringValue("name-12"), "person-12b");
            NativeIndexStore.DirectoryTreeMetrics after = store.describeTree((byte) 1, "object:people", "name");

            assertEquals(java.util.Set.of("person-12"), store.findExact((byte) 1, "object:people", "name", new StringValue("name-12")));
            assertEquals(before.height(), after.height());
            assertEquals(before.internalPageCount(), after.internalPageCount());
            assertEquals(before.leafPageCount(), after.leafPageCount());
            assertEquals(before.minLeafEntries(), after.minLeafEntries());
            assertEquals(before.maxLeafEntries(), after.maxLeafEntries());
        } finally {
            store.close();
            Files.deleteIfExists(dbPath);
        }
    }

    @Test
    public void testLeafInsertAndDeleteCanPreserveTreeShape() throws Exception {
        Path dbPath = Files.createTempFile("native-index-store-leaf-insert-delete", ".idx");
        Files.deleteIfExists(dbPath);

        ExactMatchIndexManager exact = new ExactMatchIndexManager();
        exact.ensureNamespace("object:people", List.of("name"));
        for (int i = 0; i < 24; i++) {
            exact.add("object:people", java.util.Map.of("name", new StringValue("name-" + i)), "person-" + i);
        }

        NativeIndexStore store = new NativeIndexStore(dbPath, 512);
        try {
            store.open();
            store.persist(exact, new OrderedRangeIndexManager());

            NativeIndexStore.DirectoryTreeMetrics before = store.describeTree((byte) 1, "object:people", "name");
            store.applyExactAdd("object:people", "name", new StringValue("name-12a"), "person-12a");
            assertEquals(java.util.Set.of("person-12a"), store.findExact((byte) 1, "object:people", "name", new StringValue("name-12a")));
            store.applyExactRemove("object:people", "name", new StringValue("name-12a"), "person-12a");
            NativeIndexStore.DirectoryTreeMetrics after = store.describeTree((byte) 1, "object:people", "name");

            assertEquals(java.util.Set.of(), store.findExact((byte) 1, "object:people", "name", new StringValue("name-12a")));
            assertEquals(before.height(), after.height());
            assertEquals(before.internalPageCount(), after.internalPageCount());
            assertEquals(before.leafPageCount(), after.leafPageCount());
        } finally {
            store.close();
            Files.deleteIfExists(dbPath);
        }
    }

    @Test
    public void testLeafSplitInsertCanAvoidFullTreeRebuild() throws Exception {
        Path dbPath = Files.createTempFile("native-index-store-leaf-split", ".idx");
        Files.deleteIfExists(dbPath);

        ExactMatchIndexManager exact = new ExactMatchIndexManager();
        exact.ensureNamespace("object:people", List.of("name"));
        for (int i = 0; i < 32; i++) {
            exact.add("object:people", java.util.Map.of("name", new StringValue("name-" + i)), "person-" + i);
        }

        NativeIndexStore store = new NativeIndexStore(dbPath, 224);
        try {
            store.open();
            store.persist(exact, new OrderedRangeIndexManager());

            NativeIndexStore.DirectoryTreeMetrics before = store.describeTree((byte) 1, "object:people", "name");
            store.applyExactAdd("object:people", "name", new StringValue("name-12aa"), "person-12aa");
            NativeIndexStore.DirectoryTreeMetrics after = store.describeTree((byte) 1, "object:people", "name");

            assertEquals(java.util.Set.of("person-12aa"), store.findExact((byte) 1, "object:people", "name", new StringValue("name-12aa")));
            assertEquals(before.height(), after.height());
            assertEquals(before.internalPageCount(), after.internalPageCount());
            assertEquals(before.leafPageCount() + 1, after.leafPageCount());
        } finally {
            store.close();
            Files.deleteIfExists(dbPath);
        }
    }

    @Test
    public void testRecursiveParentSplitInsertKeepsLookupsWorking() throws Exception {
        Path dbPath = Files.createTempFile("native-index-store-recursive-split", ".idx");
        Files.deleteIfExists(dbPath);

        ExactMatchIndexManager exact = new ExactMatchIndexManager();
        exact.ensureNamespace("object:people", List.of("name"));
        for (int i = 0; i < 120; i++) {
            exact.add("object:people", java.util.Map.of("name", new StringValue(String.format("name-%03d", i))), "person-" + i);
        }

        NativeIndexStore store = new NativeIndexStore(dbPath, 160);
        try {
            store.open();
            store.persist(exact, new OrderedRangeIndexManager());

            NativeIndexStore.DirectoryTreeMetrics before = store.describeTree((byte) 1, "object:people", "name");
            boolean grewInternals = false;
            for (int i = 0; i < 24 && !grewInternals; i++) {
                String insertedName = String.format("name-060x-%02d", i);
                store.applyExactAdd("object:people", "name", new StringValue(insertedName), "person-x-" + i);
                NativeIndexStore.DirectoryTreeMetrics after = store.describeTree((byte) 1, "object:people", "name");
                grewInternals = after.internalPageCount() > before.internalPageCount();
                before = after;
            }

            assertTrue(grewInternals);
            assertEquals(java.util.Set.of("person-60"), store.findExact((byte) 1, "object:people", "name", new StringValue("name-060")));
            assertEquals(java.util.Set.of("person-x-0"), store.findExact((byte) 1, "object:people", "name", new StringValue("name-060x-00")));
        } finally {
            store.close();
            Files.deleteIfExists(dbPath);
        }
    }

    @Test
    public void testRemovingLastIndexedValueRemovesFieldTree() throws Exception {
        Path dbPath = Files.createTempFile("native-index-store-remove-last-value", ".idx");
        Files.deleteIfExists(dbPath);

        ExactMatchIndexManager exact = new ExactMatchIndexManager();
        exact.ensureNamespace("object:people", List.of("name"));
        exact.add("object:people", java.util.Map.of("name", new StringValue("Ada")), "person-1");

        NativeIndexStore store = new NativeIndexStore(dbPath, 224);
        try {
            store.open();
            store.persist(exact, new OrderedRangeIndexManager());

            store.applyExactRemove("object:people", "name", new StringValue("Ada"), "person-1");

            assertEquals(java.util.Set.of(), store.findExact((byte) 1, "object:people", "name", new StringValue("Ada")));

            ExactMatchIndexManager restoredExact = new ExactMatchIndexManager();
            store.loadInto(restoredExact, new OrderedRangeIndexManager());
            assertEquals(java.util.Set.of(), restoredExact.find("object:people", "name", new StringValue("Ada")));
        } finally {
            store.close();
            Files.deleteIfExists(dbPath);
        }
    }

    @Test
    public void testDeletingValuesCanRemoveEmptyLeafFromTree() throws Exception {
        Path dbPath = Files.createTempFile("native-index-store-delete-empty-leaf", ".idx");
        Files.deleteIfExists(dbPath);

        ExactMatchIndexManager exact = new ExactMatchIndexManager();
        exact.ensureNamespace("object:people", List.of("name"));
        for (int i = 0; i < 80; i++) {
            exact.add("object:people", java.util.Map.of("name", new StringValue(String.format("name-%03d", i))), "person-" + i);
        }

        NativeIndexStore store = new NativeIndexStore(dbPath, 160);
        try {
            store.open();
            store.persist(exact, new OrderedRangeIndexManager());

            NativeIndexStore.DirectoryTreeMetrics before = store.describeTree((byte) 1, "object:people", "name");
            boolean droppedLeaf = false;
            int lastDeletedIndex = -1;
            for (int i = 0; i < 80 && !droppedLeaf; i++) {
                String name = String.format("name-%03d", i);
                store.applyExactRemove("object:people", "name", new StringValue(name), "person-" + i);
                NativeIndexStore.DirectoryTreeMetrics after = store.describeTree((byte) 1, "object:people", "name");
                droppedLeaf = after.leafPageCount() < before.leafPageCount();
                lastDeletedIndex = i;
                before = after;
            }

            assertTrue(droppedLeaf);
            assertEquals(java.util.Set.of(), store.findExact((byte) 1, "object:people", "name", new StringValue("name-000")));
            int survivingIndex = lastDeletedIndex + 1;
            assertTrue(survivingIndex < 80);
            assertEquals(
                    java.util.Set.of("person-" + survivingIndex),
                    store.findExact((byte) 1, "object:people", "name", new StringValue(String.format("name-%03d", survivingIndex)))
            );
        } finally {
            store.close();
            Files.deleteIfExists(dbPath);
        }
    }

    @Test
    public void testDeletingValuesCanMergeSiblingLeaves() throws Exception {
        Path dbPath = Files.createTempFile("native-index-store-delete-merge-leaf", ".idx");
        Files.deleteIfExists(dbPath);

        ExactMatchIndexManager exact = new ExactMatchIndexManager();
        exact.ensureNamespace("object:people", List.of("name"));
        for (int i = 0; i < 48; i++) {
            exact.add("object:people", java.util.Map.of("name", new StringValue(String.format("name-%03d", i))), "person-" + i);
        }

        NativeIndexStore store = new NativeIndexStore(dbPath, 224);
        try {
            store.open();
            store.persist(exact, new OrderedRangeIndexManager());

            NativeIndexStore.DirectoryTreeMetrics before = store.describeTree((byte) 1, "object:people", "name");
            boolean merged = false;
            int lastDeletedIndex = -1;
            for (int i = 0; i < 20 && !merged; i++) {
                String name = String.format("name-%03d", i);
                store.applyExactRemove("object:people", "name", new StringValue(name), "person-" + i);
                NativeIndexStore.DirectoryTreeMetrics after = store.describeTree((byte) 1, "object:people", "name");
                merged = after.leafPageCount() < before.leafPageCount()
                        && after.internalPageCount() == before.internalPageCount();
                lastDeletedIndex = i;
                before = after;
            }

            assertTrue(merged);
            assertEquals(java.util.Set.of(), store.findExact((byte) 1, "object:people", "name", new StringValue("name-000")));
            int survivingIndex = lastDeletedIndex + 1;
            assertTrue(survivingIndex < 48);
            assertEquals(
                    java.util.Set.of("person-" + survivingIndex),
                    store.findExact((byte) 1, "object:people", "name", new StringValue(String.format("name-%03d", survivingIndex)))
            );
        } finally {
            store.close();
            Files.deleteIfExists(dbPath);
        }
    }

    @Test
    public void testDeletingValuesCanCollapseInternalLevels() throws Exception {
        Path dbPath = Files.createTempFile("native-index-store-delete-collapse-internal", ".idx");
        Files.deleteIfExists(dbPath);

        ExactMatchIndexManager exact = new ExactMatchIndexManager();
        exact.ensureNamespace("object:people", List.of("name"));
        for (int i = 0; i < 140; i++) {
            exact.add("object:people", java.util.Map.of("name", new StringValue(String.format("name-%03d", i))), "person-" + i);
        }

        NativeIndexStore store = new NativeIndexStore(dbPath, 160);
        try {
            store.open();
            store.persist(exact, new OrderedRangeIndexManager());

            NativeIndexStore.DirectoryTreeMetrics before = store.describeTree((byte) 1, "object:people", "name");
            assertTrue(before.internalPageCount() > 1);

            boolean collapsedInternals = false;
            int lastDeletedIndex = -1;
            for (int i = 0; i < 120 && !collapsedInternals; i++) {
                String name = String.format("name-%03d", i);
                store.applyExactRemove("object:people", "name", new StringValue(name), "person-" + i);
                NativeIndexStore.DirectoryTreeMetrics after = store.describeTree((byte) 1, "object:people", "name");
                collapsedInternals = after.internalPageCount() < before.internalPageCount();
                lastDeletedIndex = i;
                before = after;
            }

            assertTrue(collapsedInternals);
            int survivingIndex = lastDeletedIndex + 1;
            assertTrue(survivingIndex < 140);
            assertEquals(
                    java.util.Set.of("person-" + survivingIndex),
                    store.findExact((byte) 1, "object:people", "name", new StringValue(String.format("name-%03d", survivingIndex)))
            );
            assertEquals(java.util.Set.of("person-139"), store.findExact((byte) 1, "object:people", "name", new StringValue("name-139")));
        } finally {
            store.close();
            Files.deleteIfExists(dbPath);
        }
    }

    @Test
    public void testDeletingValuesCanRedistributeSiblingLeaves() throws Exception {
        Path dbPath = Files.createTempFile("native-index-store-delete-redistribute-leaf", ".idx");
        Files.deleteIfExists(dbPath);

        ExactMatchIndexManager exact = new ExactMatchIndexManager();
        exact.ensureNamespace("object:people", List.of("name"));
        for (int i = 0; i < 40; i++) {
            exact.add("object:people", java.util.Map.of("name", new StringValue(String.format("name-%03d", i))), "person-" + i);
        }

        NativeIndexStore store = new NativeIndexStore(dbPath, 224);
        try {
            store.open();
            store.persist(exact, new OrderedRangeIndexManager());

            NativeIndexStore.DirectoryTreeMetrics before = store.describeTree((byte) 1, "object:people", "name");
            boolean redistributed = false;
            int lastDeletedIndex = -1;
            for (int i = 0; i < 12 && !redistributed; i++) {
                String name = String.format("name-%03d", i);
                List<Integer> beforeLeafCounts = store.describeLeafEntryCounts((byte) 1, "object:people", "name");
                store.applyExactRemove("object:people", "name", new StringValue(name), "person-" + i);
                NativeIndexStore.DirectoryTreeMetrics after = store.describeTree((byte) 1, "object:people", "name");
                List<Integer> afterLeafCounts = store.describeLeafEntryCounts((byte) 1, "object:people", "name");
                redistributed = after.leafPageCount() == before.leafPageCount()
                        && after.internalPageCount() == before.internalPageCount()
                        && beforeLeafCounts.size() > 1
                        && afterLeafCounts.size() > 1
                        && beforeLeafCounts.get(0).equals(afterLeafCounts.get(0))
                        && afterLeafCounts.get(1) == beforeLeafCounts.get(1) - 1;
                lastDeletedIndex = i;
                before = after;
            }

            assertTrue(redistributed);
            int survivingIndex = lastDeletedIndex + 1;
            assertTrue(survivingIndex < 40);
            assertEquals(
                    java.util.Set.of("person-" + survivingIndex),
                    store.findExact((byte) 1, "object:people", "name", new StringValue(String.format("name-%03d", survivingIndex)))
            );
        } finally {
            store.close();
            Files.deleteIfExists(dbPath);
        }
    }

    @Test
    public void testDeletingValuesCanRedistributeInternalSiblings() throws Exception {
        Path dbPath = Files.createTempFile("native-index-store-delete-redistribute-internal", ".idx");
        Files.deleteIfExists(dbPath);

        ExactMatchIndexManager exact = new ExactMatchIndexManager();
        exact.ensureNamespace("object:people", List.of("name"));
        for (int i = 0; i < 180; i++) {
            exact.add("object:people", java.util.Map.of("name", new StringValue(String.format("name-%03d", i))), "person-" + i);
        }

        NativeIndexStore store = new NativeIndexStore(dbPath, 160);
        try {
            store.open();
            store.persist(exact, new OrderedRangeIndexManager());

            NativeIndexStore.DirectoryTreeMetrics before = store.describeTree((byte) 1, "object:people", "name");
            assertTrue(before.internalPageCount() > 1);

            boolean redistributed = false;
            int lastDeletedIndex = -1;
            for (int i = 0; i < 120 && !redistributed; i++) {
                String name = String.format("name-%03d", i);
                List<Integer> beforeInternalCounts = store.describeInternalEntryCounts((byte) 1, "object:people", "name");
                store.applyExactRemove("object:people", "name", new StringValue(name), "person-" + i);
                NativeIndexStore.DirectoryTreeMetrics after = store.describeTree((byte) 1, "object:people", "name");
                List<Integer> afterInternalCounts = store.describeInternalEntryCounts((byte) 1, "object:people", "name");
                redistributed = after.internalPageCount() == before.internalPageCount()
                        && after.leafPageCount() < before.leafPageCount()
                        && beforeInternalCounts.size() == afterInternalCounts.size()
                        && beforeInternalCounts.size() > 1
                        && sum(beforeInternalCounts) == sum(afterInternalCounts) + 1;
                lastDeletedIndex = i;
                before = after;
            }

            assertTrue(redistributed);
            int survivingIndex = lastDeletedIndex + 1;
            assertTrue(survivingIndex < 180);
            assertEquals(
                    java.util.Set.of("person-" + survivingIndex),
                    store.findExact((byte) 1, "object:people", "name", new StringValue(String.format("name-%03d", survivingIndex)))
            );
        } finally {
            store.close();
            Files.deleteIfExists(dbPath);
        }
    }

    private int sum(List<Integer> values) {
        int total = 0;
        for (Integer value : values) {
            total += value;
        }
        return total;
    }
}
