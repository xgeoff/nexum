package biz.digitalindustry.db.engine.api;

import biz.digitalindustry.db.engine.tx.Transaction;
import biz.digitalindustry.db.engine.tx.TransactionMode;

public interface StorageEngine extends AutoCloseable {
    void open(StorageConfig config);
    boolean isOpen();
    RootHandle root();
    Transaction begin(TransactionMode mode);
    default long walSizeBytes() {
        return 0L;
    }
    default boolean needsCheckpoint() {
        return false;
    }
    default void checkpoint() {
    }
    @Override
    void close();
}
