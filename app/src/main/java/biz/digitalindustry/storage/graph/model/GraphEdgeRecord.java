package biz.digitalindustry.storage.graph.model;

public record GraphEdgeRecord(
        String id,
        String fromId,
        String toId,
        String type,
        double weight
) {
}
