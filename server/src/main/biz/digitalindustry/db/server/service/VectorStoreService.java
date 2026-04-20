package biz.digitalindustry.db.server.service;

import biz.digitalindustry.db.engine.api.StorageConfig;
import biz.digitalindustry.db.model.FieldValue;
import biz.digitalindustry.db.model.Vector;
import biz.digitalindustry.db.vector.api.VectorCollectionDefinition;
import biz.digitalindustry.db.vector.api.VectorDocument;
import biz.digitalindustry.db.vector.api.VectorDocumentMatch;
import biz.digitalindustry.db.vector.api.VectorStore;
import biz.digitalindustry.db.vector.runtime.NativeVectorStore;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Singleton;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

@Singleton
public class VectorStoreService implements VectorStore {
    private final Path dbPath;
    private final boolean memoryOnly;
    private final long pageSize;
    private final long maxWalBytes;
    private final boolean checkpointOnClose;
    private final ReentrantLock writeLock = new ReentrantLock();
    private VectorStore delegate;

    public VectorStoreService(DatabaseSettingsResolver settingsResolver) throws IOException {
        ResolvedDatabaseSettings settings = settingsResolver.resolve("vector", "./data/nexum-vector.dbs", "memory:nexum-vector");
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

    @Override
    public void registerCollection(VectorCollectionDefinition collection) {
        writeLocked(() -> {
            delegate.registerCollection(collection);
            return null;
        });
    }

    @Override
    public VectorCollectionDefinition collection(String name) {
        return readLocked(() -> delegate.collection(name));
    }

    @Override
    public VectorDocument upsert(VectorCollectionDefinition collection, VectorDocument document) {
        return writeLocked(() -> delegate.upsert(collection, document));
    }

    @Override
    public VectorDocument get(VectorCollectionDefinition collection, String key) {
        return readLocked(() -> delegate.get(collection, key));
    }

    @Override
    public List<VectorDocument> getAll(VectorCollectionDefinition collection) {
        return readLocked(() -> delegate.getAll(collection));
    }

    @Override
    public List<VectorDocument> findBy(VectorCollectionDefinition collection, String fieldName, FieldValue value) {
        return readLocked(() -> delegate.findBy(collection, fieldName, value));
    }

    @Override
    public List<VectorDocumentMatch> nearest(VectorCollectionDefinition collection, Vector query, int limit) {
        return readLocked(() -> delegate.nearest(collection, query, limit));
    }

    @Override
    public boolean delete(VectorCollectionDefinition collection, String key) {
        return writeLocked(() -> delegate.delete(collection, key));
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
    @Override
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
        delegate = new NativeVectorStore(config);
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
