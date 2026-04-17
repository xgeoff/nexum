package biz.digitalindustry.db.graph.runtime;

import biz.digitalindustry.db.engine.NativeStorageEngine;
import biz.digitalindustry.db.graph.model.GraphEdgeRecord;
import biz.digitalindustry.db.graph.model.GraphNodeRecord;
import biz.digitalindustry.db.model.Record;
import biz.digitalindustry.db.model.RecordId;
import biz.digitalindustry.db.model.StringValue;
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

public class NativeGraphStoreTest {
    private NativeGraphStore store;
    private Path dbPath;

    @After
    public void tearDown() throws Exception {
        if (store != null) {
            store.close();
        }
        if (dbPath != null) {
            Files.deleteIfExists(Path.of(dbPath + ".records"));
            Files.deleteIfExists(Path.of(dbPath + ".wal"));
            Files.deleteIfExists(dbPath);
        }
    }

    @Test
    public void testNodeAndEdgeLifecycle() throws Exception {
        dbPath = Files.createTempFile("native-graph-store", ".dbs");
        Files.deleteIfExists(dbPath);
        store = new NativeGraphStore(dbPath.toString());

        GraphNodeRecord alice = store.upsertNode("alice", "Person");
        assertEquals("alice", alice.id());
        assertEquals("Person", alice.label());

        GraphEdgeRecord knows = store.connectNodes("alice", "Person", "bob", "Person", "KNOWS", 1.5d);
        assertEquals("alice", knows.fromId());
        assertEquals("bob", knows.toId());

        List<GraphNodeRecord> nodes = store.getAllNodes();
        assertEquals(2, nodes.size());
        assertEquals(1, nodes.stream().filter(node -> node.id().equals("alice")).findFirst().orElseThrow().outgoingCount());
        assertEquals(1, nodes.stream().filter(node -> node.id().equals("bob")).findFirst().orElseThrow().incomingCount());

        List<GraphNodeRecord> outgoing = store.getOutgoingNeighbors("alice", "KNOWS");
        assertEquals(1, outgoing.size());
        assertEquals("bob", outgoing.get(0).id());

        GraphEdgeRecord updated = store.updateEdgeWeight("alice", "bob", "KNOWS", 2.25d);
        assertNotNull(updated);
        assertEquals(2.25d, updated.weight(), 0.0d);

        assertTrue(store.deleteEdge("alice", "bob", "KNOWS"));
        assertTrue(store.getOutgoingEdges("alice", null).isEmpty());

        assertTrue(store.deleteNode("bob"));
        assertNull(store.getNode("bob"));
    }

    @Test
    public void testReopenPreservesGraphData() throws Exception {
        dbPath = Files.createTempFile("native-graph-reopen", ".dbs");
        Files.deleteIfExists(dbPath);
        store = new NativeGraphStore(dbPath.toString());

        store.upsertNode("alice", "Person");
        store.connectNodes("alice", "Person", "bob", "Person", "KNOWS", 3.0d);
        store.close();
        store = null;

        store = new NativeGraphStore(dbPath.toString());

        GraphNodeRecord alice = store.getNode("alice");
        assertNotNull(alice);
        assertEquals("Person", alice.label());
        assertEquals(1, alice.outgoingCount());

        List<GraphEdgeRecord> edges = store.getAllEdges();
        assertEquals(1, edges.size());
        assertEquals("KNOWS", edges.get(0).type());
        assertEquals(3.0d, edges.get(0).weight(), 0.0d);
    }

    @Test
    public void testDeleteNodeCascadesEdges() throws Exception {
        dbPath = Files.createTempFile("native-graph-cascade", ".dbs");
        Files.deleteIfExists(dbPath);
        store = new NativeGraphStore(dbPath.toString());

        store.connectNodes("alice", "Person", "bob", "Person", "KNOWS", 1.0d);
        store.connectNodes("bob", "Person", "carol", "Person", "KNOWS", 1.0d);

        assertTrue(store.deleteNode("bob"));
        assertNull(store.getNode("bob"));
        assertTrue(store.getAllEdges().isEmpty());
        assertTrue(store.getOutgoingNeighbors("alice", null).isEmpty());
        assertTrue(store.getIncomingNeighbors("carol", null).isEmpty());
    }

    @Test
    public void testIndexedGraphLookupsReflectUpdatesDeletesAndReopen() throws Exception {
        dbPath = Files.createTempFile("native-graph-indexes", ".dbs");
        Files.deleteIfExists(dbPath);
        store = new NativeGraphStore(dbPath.toString());

        store.upsertNode("alice", "Person");
        store.upsertNode("bob", "Person");
        store.connectNodes("alice", "Person", "bob", "Person", "KNOWS", 1.0d);

        assertEquals(2, store.getNodesByLabel("Person").size());
        assertEquals(1, store.getOutgoingEdges("alice", "KNOWS").size());
        assertEquals(1, store.getIncomingEdges("bob", "KNOWS").size());

        store.updateNodeLabel("alice", "Engineer");
        store.updateEdgeWeight("alice", "bob", "KNOWS", 2.0d);

        assertEquals(1, store.getNodesByLabel("Engineer").size());
        assertEquals("alice", store.getNodesByLabel("Engineer").get(0).id());
        assertEquals(2.0d, store.getOutgoingEdges("alice", "KNOWS").get(0).weight(), 0.0d);

        store.deleteEdge("alice", "bob", "KNOWS");
        assertTrue(store.getOutgoingEdges("alice", "KNOWS").isEmpty());

        store.close();
        store = null;

        store = new NativeGraphStore(dbPath.toString());
        assertEquals(1, store.getNodesByLabel("Engineer").size());
        assertEquals("alice", store.getNode("alice").id());
        assertTrue(store.getOutgoingEdges("alice", "KNOWS").isEmpty());
    }

    @Test
    public void testReaderDuringWriteSeesLastCommittedNodeState() throws Exception {
        dbPath = Files.createTempFile("native-graph-overlap", ".dbs");
        Files.deleteIfExists(dbPath);
        store = new NativeGraphStore(dbPath.toString());

        store.upsertNode("alice", "Person");
        NativeStorageEngine engine = underlyingEngine(store);

        CountDownLatch readerDone = new CountDownLatch(1);
        AtomicReference<GraphNodeRecord> observed = new AtomicReference<>();

        try (var tx = engine.begin(biz.digitalindustry.db.engine.tx.TransactionMode.READ_WRITE)) {
            RecordId recordId = (RecordId) engine
                    .exactIndexFind("graph:nodes", "id", new StringValue("alice"))
                    .iterator()
                    .next();
            Record current = engine.recordStore().get(recordId);
            engine.recordStore().update(new Record(
                    current.id(),
                    current.type(),
                    Map.of(
                            "id", new StringValue("alice"),
                            "label", new StringValue("Engineer")
                    )
            ));

            Thread reader = new Thread(() -> {
                try {
                    observed.set(store.getNode("alice"));
                } finally {
                    readerDone.countDown();
                }
            });
            reader.start();

            assertTrue(readerDone.await(2, TimeUnit.SECONDS));
            assertEquals("Person", observed.get().label());
            tx.commit();
        }

        assertEquals("Engineer", store.getNode("alice").label());
    }

    private NativeStorageEngine underlyingEngine(NativeGraphStore store) throws Exception {
        var field = NativeGraphStore.class.getDeclaredField("storageEngine");
        field.setAccessible(true);
        return (NativeStorageEngine) field.get(store);
    }
}
