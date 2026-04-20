package biz.digitalindustry.db.server.service;

import biz.digitalindustry.db.engine.api.StorageConfig;
import io.micronaut.context.env.Environment;
import jakarta.inject.Singleton;

import java.nio.file.Path;

@Singleton
public class DatabaseSettingsResolver {
    private final Environment environment;

    public DatabaseSettingsResolver(Environment environment) {
        this.environment = environment;
    }

    public ResolvedDatabaseSettings resolve(String facade, String defaultPath, String defaultMemoryName) {
        String configuredMode = getString(facadeKey(facade, "mode"))
                .orElseGet(() -> getString(commonKey("mode")).orElse("file"));
        boolean memoryOnly = "memory".equalsIgnoreCase(configuredMode);

        String configuredPath = getString(facadeKey(facade, "path"))
                .orElseGet(() -> resolveDefaultPath(defaultPath));
        long pageSize = getLong(facadeKey(facade, "page-size"))
                .orElseGet(() -> getLong(commonKey("page-size")).orElse(StorageConfig.DEFAULT_PAGE_SIZE));
        long maxWalBytes = getLong(facadeKey(facade, "max-wal-bytes"))
                .orElseGet(() -> getLong(commonKey("max-wal-bytes")).orElse(StorageConfig.DEFAULT_MAX_WAL_BYTES));
        boolean checkpointOnClose = getBoolean(facadeKey(facade, "checkpoint-on-close"))
                .orElseGet(() -> getBoolean(commonKey("checkpoint-on-close")).orElse(true));

        return new ResolvedDatabaseSettings(
                facade,
                Path.of(configuredPath),
                memoryOnly,
                defaultMemoryName,
                pageSize,
                maxWalBytes,
                checkpointOnClose
        );
    }

    private java.util.Optional<String> getString(String key) {
        return environment.getProperty(key, String.class);
    }

    private java.util.Optional<Long> getLong(String key) {
        return environment.getProperty(key, Long.class);
    }

    private java.util.Optional<Boolean> getBoolean(String key) {
        return environment.getProperty(key, Boolean.class);
    }

    private String commonKey(String suffix) {
        return "common.db." + suffix;
    }

    private String facadeKey(String facade, String suffix) {
        return facade + ".db." + suffix;
    }

    private String resolveDefaultPath(String defaultPath) {
        java.util.Optional<String> commonDir = getString(commonKey("dir"));
        if (commonDir.isEmpty()) {
            return defaultPath;
        }
        Path defaultFile = Path.of(defaultPath).getFileName();
        if (defaultFile == null) {
            return defaultPath;
        }
        return Path.of(commonDir.get(), defaultFile.toString()).toString();
    }
}
