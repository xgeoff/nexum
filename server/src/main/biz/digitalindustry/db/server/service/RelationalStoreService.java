package biz.digitalindustry.db.server.service;

import biz.digitalindustry.db.relational.api.RelationalStore;
import biz.digitalindustry.db.relational.api.Row;
import biz.digitalindustry.db.relational.api.TableDefinition;
import biz.digitalindustry.db.relational.runtime.NativeRelationalStore;
import biz.digitalindustry.db.engine.api.StorageConfig;
import biz.digitalindustry.db.model.FieldValue;
import biz.digitalindustry.db.model.Vector;
import io.micronaut.context.annotation.Property;
import io.micronaut.context.annotation.Value;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Singleton;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import biz.digitalindustry.db.relational.api.VectorRowMatch;

@Singleton
public class RelationalStoreService implements RelationalStore {
    private final Path dbPath;
    private final boolean memoryOnly;
    private final long pageSize;
    private final long maxWalBytes;
    private final boolean checkpointOnClose;
    private final ReentrantLock writeLock = new ReentrantLock();
    private RelationalStore delegate;

    public RelationalStoreService(
            @Property(name = "relational.db.path", defaultValue = "./data/nexum-relational.dbs") String configuredPath,
            @Value("${relational.db.mode:file}") String configuredMode,
            @Value("${relational.db.page-size:8192}") long configuredPageSize,
            @Value("${relational.db.max-wal-bytes:536870912}") long configuredMaxWalBytes,
            @Value("${relational.db.checkpoint-on-close:true}") boolean configuredCheckpointOnClose
    ) throws IOException {
        memoryOnly = "memory".equalsIgnoreCase(configuredMode);
        pageSize = configuredPageSize;
        maxWalBytes = configuredMaxWalBytes;
        checkpointOnClose = configuredCheckpointOnClose;
        if (memoryOnly) {
            dbPath = Path.of("memory:nexum-relational");
        } else {
            dbPath = Path.of(configuredPath);
            Path parent = dbPath.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
        }
        openDatabase();
    }

    @Override
    public void registerTable(TableDefinition table) {
        writeLocked(() -> {
            delegate.registerTable(table);
            return null;
        });
    }

    @Override
    public TableDefinition table(String name) {
        return readLocked(() -> delegate.table(name));
    }

    @Override
    public Row upsert(TableDefinition table, Row row) {
        return writeLocked(() -> delegate.upsert(table, row));
    }

    @Override
    public Row get(TableDefinition table, String primaryKey) {
        return readLocked(() -> delegate.get(table, primaryKey));
    }

    @Override
    public List<Row> getAll(TableDefinition table) {
        return readLocked(() -> delegate.getAll(table));
    }

    @Override
    public Row findOneBy(TableDefinition table, String columnName, FieldValue value) {
        return readLocked(() -> delegate.findOneBy(table, columnName, value));
    }

    @Override
    public List<Row> findBy(TableDefinition table, String columnName, FieldValue value) {
        return readLocked(() -> delegate.findBy(table, columnName, value));
    }

    @Override
    public List<Row> findRangeBy(TableDefinition table, String columnName, FieldValue fromInclusive, FieldValue toInclusive) {
        return readLocked(() -> delegate.findRangeBy(table, columnName, fromInclusive, toInclusive));
    }

    @Override
    public List<VectorRowMatch> findNearestBy(TableDefinition table, String columnName, Vector query, int limit) {
        return readLocked(() -> delegate.findNearestBy(table, columnName, query, limit));
    }

    @Override
    public boolean delete(TableDefinition table, String primaryKey) {
        return writeLocked(() -> delegate.delete(table, primaryKey));
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
        delegate = new NativeRelationalStore(config);
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
