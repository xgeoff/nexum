package biz.digitalindustry.db.engine.api;

public interface RootHandle {
    Object get();
    <T> T get(Class<T> type);
    void set(Object root);
}
