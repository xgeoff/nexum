---
title = "Unified Engine Model"
description = "The shared storage concepts, runtime pieces, and design rules that unify object, relational, graph, and vector behavior in Nexum."
layout = "systems"
eyebrow = "Core Model"
lead = "Nexum is built as one embedded multi-model engine: record primitives, schema metadata, indexes, and recovery are shared, while facade behavior stays layered above."
metricLabel = "Model Strategy"
metricValue = "Shared primitives"
metricCopy = "Object, relational, graph, and vector behavior are implemented above the same storage-owned record, schema, and index foundation."
---

# Unified Engine Model

Status: active architecture

Development policy: only the current native on-disk format is supported before first release.

Nexum is built as a native embedded multi-model engine with one storage core and multiple façades.

Repository: [github.com/xgeoff/nexum](https://github.com/xgeoff/nexum)

## Core Model

Canonical storage concepts:

- [`RecordId.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/db/model/RecordId.java)
- [`Record.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/db/model/Record.java)
- [`FieldValue.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/db/model/FieldValue.java)
- [`ReferenceValue.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/db/model/ReferenceValue.java)
- [`Vector.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/db/model/Vector.java)
- [`VectorValue.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/db/model/VectorValue.java)
- [`EntityType.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/db/schema/EntityType.java)
- [`SchemaDefinition.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/db/schema/SchemaDefinition.java)
- [`IndexDefinition.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/db/schema/IndexDefinition.java)

The storage layer is record-oriented. Object, relational, graph, and vector behavior are implemented as façades over the same primitives.

`EntityType` remains the record-level shape used by stored records, while `SchemaDefinition` is now the persisted, facade-neutral schema contract held in the engine registry. Each facade contributes a `SchemaKind` and reconstructs its own API-specific definition from that shared metadata.

That shared engine does not imply automatic cross-facade reads. Graph records are not surfaced as relational rows or typed objects by default, and relational or object records are not surfaced as graph entities automatically. Nexum keeps those boundaries explicit because each facade introduces its own schema conventions, identity fields, and indexing behavior above the same physical storage engine.

If a product needs cross-facade access, it can implement a custom bridge or projection layer on top of the shared record and schema primitives, but that behavior is outside the default contract.

## Storage Engine

Core runtime:

- [`NativeStorageEngine.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/db/engine/NativeStorageEngine.java)
- [`PageBackedRecordStore.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/db/engine/record/PageBackedRecordStore.java)
- [`NativeIndexStore.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/db/engine/NativeIndexStore.java)
- [`PageFile.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/db/engine/page/PageFile.java)
- [`WriteAheadLog.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/db/engine/log/WriteAheadLog.java)

Current engine properties:

- page-backed slotted record storage
- allocator metadata and slot reuse
- checkpointed engine metadata
- WAL-backed transaction recovery
- exact-match and ordered/range indexes
- vector nearest-neighbor indexes
- persisted native index pages with recursive directory trees
- direct native exact and range reads from persisted index structures

## Facades

Object facade:

- [`ObjectStore.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/db/object/api/ObjectStore.java)
- [`ObjectType.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/db/object/api/ObjectType.java)
- [`ObjectCodec.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/db/object/api/ObjectCodec.java)
- [`NativeObjectStore.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/db/object/runtime/NativeObjectStore.java)

Relational facade:

- [`RelationalStore.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/db/relational/api/RelationalStore.java)
- [`TableDefinition.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/db/relational/api/TableDefinition.java)
- [`Row.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/db/relational/api/Row.java)
- [`NativeRelationalStore.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/db/relational/runtime/NativeRelationalStore.java)

Graph facade:

- [`GraphStore.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/db/graph/api/GraphStore.java)
- [`GraphNodeRecord.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/db/graph/model/GraphNodeRecord.java)
- [`GraphEdgeRecord.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/db/graph/model/GraphEdgeRecord.java)
- [`NativeGraphStore.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/db/graph/runtime/NativeGraphStore.java)

Vector support:

- [`VectorCollectionDefinition.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/db/vector/api/VectorCollectionDefinition.java)
- [`NativeVectorStore.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/db/vector/runtime/NativeVectorStore.java)
- [`VectorIndex.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/db/index/VectorIndex.java)
- [`VectorMatch.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/db/index/VectorMatch.java)
- [`FlatVectorIndex.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/db/vector/FlatVectorIndex.java)
- [`VectorIndexDefinition.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/db/vector/VectorIndexDefinition.java)
- [`Distances.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/db/vector/Distances.java)

## Query Module

Query providers live in the product layer, not the server layer.

- [`QueryProvider.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/db/query/QueryProvider.java)
- [`QueryProviderRegistry.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/db/query/QueryProviderRegistry.java)
- [`CypherQueryProvider.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/db/query/cypher/CypherQueryProvider.java)
- [`SqlQueryProvider.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/db/query/sql/SqlQueryProvider.java)
- [`VectorQueryProvider.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/db/query/vector/VectorQueryProvider.java)

Dependency direction:

- Cypher provider -> graph facade
- SQL provider -> relational facade
- Vector provider -> vector/relational nearest-neighbor search
- server -> query providers

## Shared Indexing

Shared index layers:

- [`ExactMatchIndexManager.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/db/engine/ExactMatchIndexManager.java)
- [`OrderedRangeIndexManager.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/db/engine/OrderedRangeIndexManager.java)
- [`VectorIndexManager.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/db/engine/VectorIndexManager.java)
- [`ExactMatchIndex.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/db/index/ExactMatchIndex.java)
- [`OrderedRangeIndex.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/db/index/OrderedRangeIndex.java)
- [`VectorIndex.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/db/index/VectorIndex.java)

All three façades share the same storage-owned exact and ordered lookup path, and vector search now plugs into that same engine-owned indexing layer rather than living in a separate subsystem.

## Concurrency

Nexum currently standardizes on single-writer/multi-reader access.

- readers may overlap with the single writer
- readers see last committed state only
- the writer works against a private overlay until commit publishes

- concurrent readers are allowed at the façade boundary
- writes are exclusive
- the engine exposes `READ_ONLY`, `READ_WRITE`, and `SERIALIZABLE` transaction modes
- `SERIALIZABLE` currently maps to the same exclusive writer lane as `READ_WRITE`

See [`concurrency-model.md`](/concurrency-model.html) for the operational contract.

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
