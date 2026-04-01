package biz.digitalindustry.graph.contract;

import biz.digitalindustry.graph.api.GraphStore;
import biz.digitalindustry.graph.model.GraphEdgeRecord;
import biz.digitalindustry.graph.model.GraphNodeRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.Assert.*;

public abstract class GraphStoreContractTest {
    protected GraphStore store;
    protected Path dbPath;

    protected abstract GraphStore createStore(String path);

    @Before
    public void setUp() throws Exception {
        dbPath = Files.createTempFile("graph-store-contract", ".dbs");
        Files.deleteIfExists(dbPath);
        store = createStore(dbPath.toString());
    }

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
    public void testNodeAndEdgeLifecycle() {
        GraphNodeRecord alice = store.upsertNode("alice", "Person");
        assertEquals("alice", alice.id());
        assertEquals("Person", alice.label());

        GraphEdgeRecord knows = store.connectNodes("alice", "Person", "bob", "Person", "KNOWS", 2.0d);
        assertEquals("alice", knows.fromId());
        assertEquals("bob", knows.toId());
        assertEquals("KNOWS", knows.type());

        assertEquals(2, store.getAllNodes().size());
        assertEquals(1, store.getAllEdges().size());

        GraphNodeRecord updatedNode = store.updateNodeLabel("alice", "Engineer");
        assertNotNull(updatedNode);
        assertEquals("Engineer", updatedNode.label());

        GraphEdgeRecord updatedEdge = store.updateEdgeWeight("alice", "bob", "KNOWS", 4.5d);
        assertNotNull(updatedEdge);
        assertEquals(4.5d, updatedEdge.weight(), 0.0d);

        assertTrue(store.deleteEdge("alice", "bob", "KNOWS"));
        assertTrue(store.getAllEdges().isEmpty());

        assertTrue(store.deleteNode("bob"));
        assertNull(store.getNode("bob"));
    }

    @Test
    public void testTraversalAndCascadeDelete() {
        store.connectNodes("alice", "Person", "bob", "Person", "KNOWS", 1.0d);
        store.connectNodes("bob", "Person", "carol", "Person", "KNOWS", 1.0d);

        List<GraphNodeRecord> outgoing = store.getOutgoingNeighbors("alice", "KNOWS");
        assertEquals(1, outgoing.size());
        assertEquals("bob", outgoing.get(0).id());

        List<GraphNodeRecord> incoming = store.getIncomingNeighbors("carol", "KNOWS");
        assertEquals(1, incoming.size());
        assertEquals("bob", incoming.get(0).id());

        assertTrue(store.deleteNode("bob"));
        assertTrue(store.getAllEdges().isEmpty());
        assertTrue(store.getOutgoingNeighbors("alice", null).isEmpty());
        assertTrue(store.getIncomingNeighbors("carol", null).isEmpty());
    }

    @Test
    public void testReopenPreservesGraphData() throws Exception {
        store.upsertNode("alice", "Person");
        store.connectNodes("alice", "Person", "bob", "Person", "KNOWS", 3.0d);
        store.close();
        store = createStore(dbPath.toString());

        GraphNodeRecord alice = store.getNode("alice");
        assertNotNull(alice);
        assertEquals("Person", alice.label());
        assertEquals(1, alice.outgoingCount());

        List<GraphEdgeRecord> edges = store.getAllEdges();
        assertEquals(1, edges.size());
        assertEquals("alice", edges.get(0).fromId());
        assertEquals("bob", edges.get(0).toId());
        assertEquals(3.0d, edges.get(0).weight(), 0.0d);
    }
}
