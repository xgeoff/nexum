package biz.digitalindustry.db.engine.api;

public record StorageConfig(
        String path,
        long pagePoolSize,
        long maxWalBytes,
        boolean checkpointOnClose,
        boolean memoryOnly
) {
    public static final long DEFAULT_MAX_WAL_BYTES = 512L * 1024L * 1024L;
    public static final long DEFAULT_PAGE_SIZE = 8192L;
    public static final String DEFAULT_MEMORY_NAME = "memory:nexum";

    public StorageConfig(String path, long pagePoolSize) {
        this(path, pagePoolSize, DEFAULT_MAX_WAL_BYTES, true, false);
    }

    public StorageConfig(String path, long pagePoolSize, long maxWalBytes, boolean checkpointOnClose) {
        this(path, pagePoolSize, maxWalBytes, checkpointOnClose, false);
    }

    public static StorageConfig file(String path) {
        return new StorageConfig(path, DEFAULT_PAGE_SIZE);
    }

    public static StorageConfig file(String path, long pagePoolSize) {
        return new StorageConfig(path, pagePoolSize);
    }

    public static StorageConfig file(String path, long pagePoolSize, long maxWalBytes, boolean checkpointOnClose) {
        return new StorageConfig(path, pagePoolSize, maxWalBytes, checkpointOnClose, false);
    }

    public static StorageConfig memory() {
        return memory(DEFAULT_MEMORY_NAME, DEFAULT_PAGE_SIZE, DEFAULT_MAX_WAL_BYTES);
    }

    public static StorageConfig memory(String name) {
        return memory(name, DEFAULT_PAGE_SIZE, DEFAULT_MAX_WAL_BYTES);
    }

    public static StorageConfig memory(long pagePoolSize) {
        return memory(DEFAULT_MEMORY_NAME, pagePoolSize, DEFAULT_MAX_WAL_BYTES);
    }

    public static StorageConfig memory(String name, long pagePoolSize, long maxWalBytes) {
        return new StorageConfig(name, pagePoolSize, maxWalBytes, false, true);
    }
}
