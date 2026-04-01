package biz.digitalindustry.storage.api;

public interface RootHandle {
    Object get();
    <T> T get(Class<T> type);
    void set(Object root);
}
