package biz.digitalindustry.db.vector.api;

public record VectorDocumentMatch(
        VectorDocument document,
        float distance
) {
}
