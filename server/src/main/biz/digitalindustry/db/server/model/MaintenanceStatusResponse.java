package biz.digitalindustry.db.server.model;

public record MaintenanceStatusResponse(
        long walSizeBytes,
        boolean checkpointRequested
) {
}
