package biz.digitalindustry.db.server.service;

import java.nio.file.Path;

record ResolvedDatabaseSettings(
        String facade,
        Path dbPath,
        boolean memoryOnly,
        String memoryName,
        long pageSize,
        long maxWalBytes,
        boolean checkpointOnClose
) {
}
