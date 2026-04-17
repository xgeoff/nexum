package biz.digitalindustry.db.graph.model;

public record GraphNodeRecord(
        String id,
        String label,
        int outgoingCount,
        int incomingCount
) {
}
