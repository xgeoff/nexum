package biz.digitalindustry.storage.graph.model;

public record GraphNodeRecord(
        String id,
        String label,
        int outgoingCount,
        int incomingCount
) {
}
