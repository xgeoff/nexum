package biz.digitalindustry.db.server.service;

import biz.digitalindustry.graph.engine.NativeGraphStore;
import biz.digitalindustry.graph.model.GraphEdgeRecord;
import biz.digitalindustry.graph.model.GraphNodeRecord;
import io.micronaut.context.annotation.Property;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Singleton;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

@Singleton
public class GraphStore implements biz.digitalindustry.graph.api.GraphStore {
    private final Path dbPath;
    private final boolean deleteOnClose;
    private biz.digitalindustry.graph.api.GraphStore delegate;

    public GraphStore(
            @Property(name = "graph.db.path", defaultValue = "") String configuredPath
    ) throws IOException {
        if (configuredPath == null || configuredPath.isBlank()) {
            dbPath = Files.createTempFile("nexum-graph", ".dbs");
            deleteOnClose = true;
        } else {
            dbPath = Path.of(configuredPath);
            Path parent = dbPath.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            deleteOnClose = false;
        }
        openDatabase();
    }

    public synchronized GraphNodeRecord upsertNode(String id, String label) {
        return delegate.upsertNode(id, label);
    }

    public synchronized GraphNodeRecord updateNodeLabel(String id, String label) {
        return delegate.updateNodeLabel(id, label);
    }

    public synchronized GraphNodeRecord getNode(String id) {
        return delegate.getNode(id);
    }

    public synchronized List<GraphNodeRecord> getAllNodes() {
        return delegate.getAllNodes();
    }

    public synchronized List<GraphNodeRecord> getNodesByLabel(String label) {
        return delegate.getNodesByLabel(label);
    }

    public synchronized GraphEdgeRecord connectNodes(String fromId, String fromLabel, String toId, String toLabel, String type, double weight) {
        return delegate.connectNodes(fromId, fromLabel, toId, toLabel, type, weight);
    }

    public synchronized List<GraphEdgeRecord> getAllEdges() {
        return delegate.getAllEdges();
    }

    public synchronized List<GraphEdgeRecord> getOutgoingEdges(String nodeId, String type) {
        return delegate.getOutgoingEdges(nodeId, type);
    }

    public synchronized List<GraphEdgeRecord> getIncomingEdges(String nodeId, String type) {
        return delegate.getIncomingEdges(nodeId, type);
    }

    public synchronized List<GraphNodeRecord> getOutgoingNeighbors(String nodeId, String type) {
        return delegate.getOutgoingNeighbors(nodeId, type);
    }

    public synchronized List<GraphNodeRecord> getIncomingNeighbors(String nodeId, String type) {
        return delegate.getIncomingNeighbors(nodeId, type);
    }

    public synchronized GraphEdgeRecord updateEdgeWeight(String fromId, String toId, String type, double weight) {
        return delegate.updateEdgeWeight(fromId, toId, type, weight);
    }

    public synchronized boolean deleteEdge(String fromId, String toId, String type) {
        return delegate.deleteEdge(fromId, toId, type);
    }

    public synchronized boolean deleteNode(String id) {
        return delegate.deleteNode(id);
    }

    public synchronized void reset() throws IOException {
        closeDatabase();
        Files.deleteIfExists(dbPath);
        Files.deleteIfExists(Path.of(dbPath + ".records"));
        Files.deleteIfExists(Path.of(dbPath + ".wal"));
        openDatabase();
    }

    @PreDestroy
    public synchronized void close() {
        closeDatabase();
        if (deleteOnClose) {
            try {
                Files.deleteIfExists(Path.of(dbPath + ".records"));
                Files.deleteIfExists(Path.of(dbPath + ".wal"));
                Files.deleteIfExists(dbPath);
            } catch (IOException e) {
                throw new RuntimeException("Failed to delete graph store file " + dbPath, e);
            }
        }
    }

    private void openDatabase() {
        delegate = new NativeGraphStore(dbPath.toString());
    }

    private void closeDatabase() {
        if (delegate != null) {
            delegate.close();
            delegate = null;
        }
    }
}
