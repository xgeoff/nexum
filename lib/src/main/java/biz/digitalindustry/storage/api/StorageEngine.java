package biz.digitalindustry.storage.api;

import biz.digitalindustry.storage.tx.Transaction;
import biz.digitalindustry.storage.tx.TransactionMode;

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
