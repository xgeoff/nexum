---
title = "Relational Facade"
description = "Table-and-row storage over the shared native engine with exact, range, and vector lookup on declared indexes."
layout = "reference"
eyebrow = "API Reference"
lead = "The relational facade is intentionally narrow and storage-focused, giving tables, rows, and index-backed lookups, including nearest-neighbor vector search, without pretending to be a full SQL engine."
statusLabel = "Relational Scope"
accent = "Table primitives over one engine"
---

# Relational Facade

Status: active native implementation

This facade provides table-and-row storage on top of the native record engine.

Source package: [github.com/xgeoff/nexum/tree/main/lib/src/main/java/biz/digitalindustry/db/relational](https://github.com/xgeoff/nexum/tree/main/lib/src/main/java/biz/digitalindustry/db/relational)

## Key Types

- [`RelationalStore.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/db/relational/api/RelationalStore.java)
- [`TableDefinition.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/db/relational/api/TableDefinition.java)
- [`Row.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/db/relational/api/Row.java)
- [`NativeRelationalStore.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/db/relational/runtime/NativeRelationalStore.java)
- [`VectorRowMatch.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/db/relational/api/VectorRowMatch.java)

## Design

The current relational layer is intentionally narrow. It is a storage facade, not a SQL engine.

It uses:

- explicit `TableDefinition`
- explicit primary-key column selection
- row storage as `Map<String, FieldValue>`
- exact-match lookup
- ordered/range lookup for declared ordered indexes
- nearest-neighbor lookup for declared vector indexes

Table definitions are now restored from the persisted engine schema on reopen, so a reopened relational store can recover registered tables without manual re-registration.

Each stored row also carries a reserved internal key, `rowKey`, derived from the primary-key value.

## Capabilities

Current API surface:

- register table
- upsert row by primary key
- get row by primary key
- scan all rows
- exact-match lookup by column value
- ordered/range lookup by column value
- nearest-neighbor lookup by vector column
- delete by primary key

## Indexing

The relational facade uses shared engine-owned indexes for:

- exact-match single-field indexes
- ordered/range single-field indexes
- vector single-field indexes
- the reserved `rowKey` field

Those indexes are persisted in the native `.indexes` sidecar and can be queried directly from persisted index pages during non-transactional reads.

## Example

```java
try (biz.digitalindustry.db.relational.api.RelationalStore store =
             new biz.digitalindustry.db.relational.runtime.NativeRelationalStore("data/relational.dbs")) {
    store.registerTable(users);

    store.upsert(users, new Row("u1", Map.of(
            "id", new StringValue("u1"),
            "name", new StringValue("Ada"),
            "age", new LongValue(37)
    )));

    Row row = store.get(users, "u1");
    List<Row> matches = store.findBy(users, "age", new LongValue(37));
    List<Row> range = store.findRangeBy(users, "age", new LongValue(30), new LongValue(40));
}
```

Vector example:

```java
TableDefinition embeddings = new TableDefinition(
        "embeddings",
        "id",
        List.of(
                new FieldDefinition("id", ValueType.STRING, true, false),
                FieldDefinition.vector("embedding", 3, true)
        ),
        List.of(
                IndexDefinition.vector("embeddings_embedding_idx", "embedding", 3, "euclidean")
        )
);

store.registerTable(embeddings);

var matches = store.findNearestBy(
        embeddings,
        "embedding",
        Vector.of(1.0f, 0.0f, 0.0f),
        10
);
```

## Current Limits

- no SQL planner or optimizer
- no joins or aggregations
- no composite primary keys
- no foreign-key enforcement
- SQL support is a narrow provider layer, not full ANSI SQL
- vector search is currently exposed through the direct relational API, not through the default SQL subset
- some structural delete cases in the native index tree still use rebuild fallback
