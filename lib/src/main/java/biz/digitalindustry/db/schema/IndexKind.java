package biz.digitalindustry.db.schema;

public enum IndexKind {
    PRIMARY,
    UNIQUE,
    NON_UNIQUE,
    ORDERED_RANGE,
    VECTOR,
    REFERENCE,
    ADJACENCY
}
