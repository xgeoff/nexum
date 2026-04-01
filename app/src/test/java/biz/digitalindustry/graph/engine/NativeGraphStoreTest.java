package biz.digitalindustry.graph.engine;

import biz.digitalindustry.graph.model.GraphEdgeRecord;
import biz.digitalindustry.graph.model.GraphNodeRecord;
import org.junit.After;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

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
}
