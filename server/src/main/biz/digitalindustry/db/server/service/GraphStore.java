package biz.digitalindustry.db.server.service;

import biz.digitalindustry.db.graph.runtime.NativeGraphStore;
import biz.digitalindustry.db.graph.model.GraphEdgeRecord;
import biz.digitalindustry.db.graph.model.GraphNodeRecord;
import biz.digitalindustry.db.engine.api.StorageConfig;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Singleton;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

@Singleton
public class GraphStore implements biz.digitalindustry.db.graph.api.GraphStore {
    private final Path dbPath;
    private final boolean memoryOnly;
    private final long pageSize;
    private final long maxWalBytes;
    private final boolean checkpointOnClose;
    private final ReentrantLock writeLock = new ReentrantLock();
    private biz.digitalindustry.db.graph.api.GraphStore delegate;

    public GraphStore(DatabaseSettingsResolver settingsResolver) throws IOException {
        ResolvedDatabaseSettings settings = settingsResolver.resolve("graph", "./data/nexum-graph.dbs", "memory:nexum-graph");
        memoryOnly = settings.memoryOnly();
        pageSize = settings.pageSize();
        maxWalBytes = settings.maxWalBytes();
        checkpointOnClose = settings.checkpointOnClose();
        if (memoryOnly) {
            dbPath = Path.of(settings.memoryName());
        } else {
            dbPath = settings.dbPath();
            Path parent = dbPath.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
        }
        openDatabase();
    }

    public GraphNodeRecord upsertNode(String id, String label) {
        return writeLocked(() -> delegate.upsertNode(id, label));
    }

    public GraphNodeRecord updateNodeLabel(String id, String label) {
        return writeLocked(() -> delegate.updateNodeLabel(id, label));
    }

    public GraphNodeRecord getNode(String id) {
        return readLocked(() -> delegate.getNode(id));
    }

    public List<GraphNodeRecord> getAllNodes() {
        return readLocked(delegate::getAllNodes);
    }

    public List<GraphNodeRecord> getNodesByLabel(String label) {
        return readLocked(() -> delegate.getNodesByLabel(label));
    }

    public GraphEdgeRecord connectNodes(String fromId, String fromLabel, String toId, String toLabel, String type, double weight) {
        return writeLocked(() -> delegate.connectNodes(fromId, fromLabel, toId, toLabel, type, weight));
    }

    public List<GraphEdgeRecord> getAllEdges() {
        return readLocked(delegate::getAllEdges);
    }

    public List<GraphEdgeRecord> getOutgoingEdges(String nodeId, String type) {
        return readLocked(() -> delegate.getOutgoingEdges(nodeId, type));
    }

    public List<GraphEdgeRecord> getIncomingEdges(String nodeId, String type) {
        return readLocked(() -> delegate.getIncomingEdges(nodeId, type));
    }

    public List<GraphNodeRecord> getOutgoingNeighbors(String nodeId, String type) {
        return readLocked(() -> delegate.getOutgoingNeighbors(nodeId, type));
    }

    public List<GraphNodeRecord> getIncomingNeighbors(String nodeId, String type) {
        return readLocked(() -> delegate.getIncomingNeighbors(nodeId, type));
    }

    public GraphEdgeRecord updateEdgeWeight(String fromId, String toId, String type, double weight) {
        return writeLocked(() -> delegate.updateEdgeWeight(fromId, toId, type, weight));
    }

    public boolean deleteEdge(String fromId, String toId, String type) {
        return writeLocked(() -> delegate.deleteEdge(fromId, toId, type));
    }

    public boolean deleteNode(String id) {
        return writeLocked(() -> delegate.deleteNode(id));
    }

    public long walSizeBytes() {
        return readLocked(() -> ((NativeGraphStore) delegate).walSizeBytes());
    }

    public boolean needsCheckpoint() {
        return readLocked(() -> ((NativeGraphStore) delegate).needsCheckpoint());
    }

    public void checkpoint() {
        writeLocked(() -> {
            ((NativeGraphStore) delegate).checkpoint();
            return null;
        });
    }

    public void reset() throws IOException {
        writeLock.lock();
        try {
            closeDatabase();
            if (!memoryOnly) {
                Files.deleteIfExists(dbPath);
                Files.deleteIfExists(Path.of(dbPath + ".records"));
                Files.deleteIfExists(Path.of(dbPath + ".wal"));
                Files.deleteIfExists(Path.of(dbPath + ".indexes"));
            }
            openDatabase();
        } finally {
            writeLock.unlock();
        }
    }

    @PreDestroy
    public void close() {
        writeLock.lock();
        try {
            closeDatabase();
        } finally {
            writeLock.unlock();
        }
    }

    private void openDatabase() {
        StorageConfig config = memoryOnly
                ? StorageConfig.memory(dbPath.toString(), pageSize, maxWalBytes)
                : StorageConfig.file(dbPath.toString(), pageSize, maxWalBytes, checkpointOnClose);
        delegate = new NativeGraphStore(config);
    }

    private void closeDatabase() {
        if (delegate != null) {
            delegate.close();
            delegate = null;
        }
    }

    private <T> T readLocked(Supplier<T> action) {
        return action.get();
    }

    private <T> T writeLocked(Supplier<T> action) {
        writeLock.lock();
        try {
            return action.get();
        } finally {
            writeLock.unlock();
        }
    }
}
