package biz.digitalindustry.storage.server.model;

public record MaintenanceStatusResponse(
        long walSizeBytes,
        boolean checkpointRequested
) {
}
