package biz.digitalindustry.db.graph.model;

public record GraphEdgeRecord(
        String id,
        String fromId,
        String toId,
        String type,
        double weight
) {
}
