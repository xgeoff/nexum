# Relational Facade

Status: active native implementation

This facade provides table-and-row storage on top of the native record engine.

## Key Types

- [`RelationalStore.java`](../app/src/main/java/biz/digitalindustry/relational/api/RelationalStore.java)
- [`TableDefinition.java`](../app/src/main/java/biz/digitalindustry/relational/api/TableDefinition.java)
- [`Row.java`](../app/src/main/java/biz/digitalindustry/relational/api/Row.java)
- [`NativeRelationalStore.java`](../app/src/main/java/biz/digitalindustry/relational/engine/NativeRelationalStore.java)

## Design

The current relational layer is intentionally narrow. It is a storage facade, not a SQL engine.

It uses:

- explicit `TableDefinition`
- explicit primary-key column selection
- row storage as `Map<String, FieldValue>`
- exact-match lookup
- ordered/range lookup for declared ordered indexes

Each stored row also carries a reserved internal key, `rowKey`, derived from the primary-key value.

## Capabilities

Current API surface:

- register table
- upsert row by primary key
- get row by primary key
- scan all rows
- exact-match lookup by column value
- ordered/range lookup by column value
- delete by primary key

## Indexing

The relational facade uses shared engine-owned indexes for:

- exact-match single-field indexes
- ordered/range single-field indexes
- the reserved `rowKey` field

Those indexes are persisted in the native `.indexes` sidecar and can be queried directly from persisted index pages during non-transactional reads.

## Example

```java
try (RelationalStore store = new NativeRelationalStore("data/relational.dbs")) {
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

## Current Limits

- no SQL planner or optimizer
- no joins or aggregations
- no composite primary keys
- no foreign-key enforcement
- some structural delete cases in the native index tree still use rebuild fallback
