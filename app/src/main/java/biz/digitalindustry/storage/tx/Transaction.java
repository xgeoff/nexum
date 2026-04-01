package biz.digitalindustry.storage.tx;

public interface Transaction extends AutoCloseable {
    void commit();
    void rollback();

    @Override
    void close();
}
