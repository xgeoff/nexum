---
title = "Unified Engine Model"
description = "The shared storage concepts, runtime pieces, and design rules that unify object, relational, and graph behavior in Nexum."
layout = "systems"
eyebrow = "Core Model"
lead = "Nexum is built as one embedded multi-model engine: record primitives, schema metadata, indexes, and recovery are shared, while facade behavior stays layered above."
metricLabel = "Model Strategy"
metricValue = "Shared primitives"
metricCopy = "Object, relational, and graph behavior are implemented above the same storage-owned record, schema, and index foundation."
---

# Unified Engine Model

Status: active architecture

Development policy: only the current native on-disk format is supported before first release.

Nexum is built as a native embedded multi-model engine with one storage core and multiple façades.

## Core Model

Canonical storage concepts:

- [`RecordId.java`](../app/src/main/java/biz/digitalindustry/storage/model/RecordId.java)
- [`Record.java`](../app/src/main/java/biz/digitalindustry/storage/model/Record.java)
- [`FieldValue.java`](../app/src/main/java/biz/digitalindustry/storage/model/FieldValue.java)
- [`ReferenceValue.java`](../app/src/main/java/biz/digitalindustry/storage/model/ReferenceValue.java)
- [`EntityType.java`](../app/src/main/java/biz/digitalindustry/storage/schema/EntityType.java)
- [`IndexDefinition.java`](../app/src/main/java/biz/digitalindustry/storage/schema/IndexDefinition.java)

The storage layer is record-oriented. Object, relational, and graph behavior are implemented as façades over the same primitives.

## Storage Engine

Core runtime:

- [`NativeStorageEngine.java`](../app/src/main/java/biz/digitalindustry/storage/engine/NativeStorageEngine.java)
- [`PageBackedRecordStore.java`](../app/src/main/java/biz/digitalindustry/storage/store/PageBackedRecordStore.java)
- [`NativeIndexStore.java`](../app/src/main/java/biz/digitalindustry/storage/engine/NativeIndexStore.java)
- [`PageFile.java`](../app/src/main/java/biz/digitalindustry/storage/page/PageFile.java)
- [`WriteAheadLog.java`](../app/src/main/java/biz/digitalindustry/storage/log/WriteAheadLog.java)

Current engine properties:

- page-backed slotted record storage
- allocator metadata and slot reuse
- checkpointed engine metadata
- WAL-backed transaction recovery
- exact-match and ordered/range indexes
- persisted native index pages with recursive directory trees
- direct native exact and range reads from persisted index structures

## Facades

Object facade:

- [`ObjectStore.java`](../app/src/main/java/biz/digitalindustry/storage/object/api/ObjectStore.java)
- [`ObjectType.java`](../app/src/main/java/biz/digitalindustry/storage/object/api/ObjectType.java)
- [`ObjectCodec.java`](../app/src/main/java/biz/digitalindustry/storage/object/api/ObjectCodec.java)
- [`NativeObjectStore.java`](../app/src/main/java/biz/digitalindustry/storage/object/engine/NativeObjectStore.java)

Relational facade:

- [`RelationalStore.java`](../app/src/main/java/biz/digitalindustry/storage/relational/api/RelationalStore.java)
- [`TableDefinition.java`](../app/src/main/java/biz/digitalindustry/storage/relational/api/TableDefinition.java)
- [`Row.java`](../app/src/main/java/biz/digitalindustry/storage/relational/api/Row.java)
- [`NativeRelationalStore.java`](../app/src/main/java/biz/digitalindustry/storage/relational/engine/NativeRelationalStore.java)

Graph facade:

- [`GraphStore.java`](../app/src/main/java/biz/digitalindustry/storage/graph/api/GraphStore.java)
- [`GraphNodeRecord.java`](../app/src/main/java/biz/digitalindustry/storage/graph/model/GraphNodeRecord.java)
- [`GraphEdgeRecord.java`](../app/src/main/java/biz/digitalindustry/storage/graph/model/GraphEdgeRecord.java)
- [`NativeGraphStore.java`](../app/src/main/java/biz/digitalindustry/storage/graph/engine/NativeGraphStore.java)

## Query Module

Query providers live in the product layer, not the server layer.

- [`QueryProvider.java`](../app/src/main/java/biz/digitalindustry/storage/query/QueryProvider.java)
- [`QueryProviderRegistry.java`](../app/src/main/java/biz/digitalindustry/storage/query/QueryProviderRegistry.java)
- [`CypherQueryProvider.java`](../app/src/main/java/biz/digitalindustry/storage/query/cypher/CypherQueryProvider.java)
- [`SqlQueryProvider.java`](../app/src/main/java/biz/digitalindustry/storage/query/sql/SqlQueryProvider.java)

Dependency direction:

- Cypher provider -> graph facade
- SQL provider -> relational facade
- server -> query providers

## Shared Indexing

Shared index layers:

- [`ExactMatchIndexManager.java`](../app/src/main/java/biz/digitalindustry/storage/engine/ExactMatchIndexManager.java)
- [`OrderedRangeIndexManager.java`](../app/src/main/java/biz/digitalindustry/storage/engine/OrderedRangeIndexManager.java)
- [`ExactMatchIndex.java`](../app/src/main/java/biz/digitalindustry/storage/index/ExactMatchIndex.java)
- [`OrderedRangeIndex.java`](../app/src/main/java/biz/digitalindustry/storage/index/OrderedRangeIndex.java)

All three façades share the same storage-owned exact and ordered lookup path.

## Concurrency

Nexum currently standardizes on single-writer/multi-reader access.

- readers may overlap with the single writer
- readers see last committed state only
- the writer works against a private overlay until commit publishes

- concurrent readers are allowed at the façade boundary
- writes are exclusive
- the engine exposes `READ_ONLY`, `READ_WRITE`, and `SERIALIZABLE` transaction modes
- `SERIALIZABLE` currently maps to the same exclusive writer lane as `READ_WRITE`

See [`concurrency-model.md`](./concurrency-model.md) for the operational contract.

## Design Rules

- keep the engine record-based, not facade-specific
- keep schema explicit
- keep persistence semantics explicit
- optimize storage and index structures underneath stable APIs
- support only the current native file format until the first release

## Current Limits

- some native index delete scenarios still fall back to field rebuild
- the query providers expose only a narrow explicit subset
- SQL is single-table and pattern-driven, not full ANSI SQL
- object and relational APIs are explicit, not reflective or inferred
- the concurrency model is SWMR rather than MVCC or multi-writer
