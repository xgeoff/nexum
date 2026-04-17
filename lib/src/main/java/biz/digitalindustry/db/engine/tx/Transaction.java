package biz.digitalindustry.db.engine.tx;

public interface Transaction extends AutoCloseable {
    void commit();
    void rollback();

    @Override
    void close();
}
