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

- [`ObjectStore.java`](../app/src/main/java/biz/digitalindustry/object/api/ObjectStore.java)
- [`ObjectType.java`](../app/src/main/java/biz/digitalindustry/object/api/ObjectType.java)
- [`ObjectCodec.java`](../app/src/main/java/biz/digitalindustry/object/api/ObjectCodec.java)
- [`NativeObjectStore.java`](../app/src/main/java/biz/digitalindustry/object/engine/NativeObjectStore.java)

Relational facade:

- [`RelationalStore.java`](../app/src/main/java/biz/digitalindustry/relational/api/RelationalStore.java)
- [`TableDefinition.java`](../app/src/main/java/biz/digitalindustry/relational/api/TableDefinition.java)
- [`Row.java`](../app/src/main/java/biz/digitalindustry/relational/api/Row.java)
- [`NativeRelationalStore.java`](../app/src/main/java/biz/digitalindustry/relational/engine/NativeRelationalStore.java)

Graph facade:

- [`GraphStore.java`](../app/src/main/java/biz/digitalindustry/graph/api/GraphStore.java)
- [`GraphNodeRecord.java`](../app/src/main/java/biz/digitalindustry/graph/model/GraphNodeRecord.java)
- [`GraphEdgeRecord.java`](../app/src/main/java/biz/digitalindustry/graph/model/GraphEdgeRecord.java)
- [`NativeGraphStore.java`](../app/src/main/java/biz/digitalindustry/graph/engine/NativeGraphStore.java)

## Shared Indexing

Shared index layers:

- [`ExactMatchIndexManager.java`](../app/src/main/java/biz/digitalindustry/storage/engine/ExactMatchIndexManager.java)
- [`OrderedRangeIndexManager.java`](../app/src/main/java/biz/digitalindustry/storage/engine/OrderedRangeIndexManager.java)
- [`ExactMatchIndex.java`](../app/src/main/java/biz/digitalindustry/storage/index/ExactMatchIndex.java)
- [`OrderedRangeIndex.java`](../app/src/main/java/biz/digitalindustry/storage/index/OrderedRangeIndex.java)

All three façades share the same storage-owned exact and ordered lookup path.

## Design Rules

- keep the engine record-based, not facade-specific
- keep schema explicit
- keep persistence semantics explicit
- optimize storage and index structures underneath stable APIs
- support only the current native file format until the first release

## Current Limits

- some native index delete scenarios still fall back to field rebuild
- the query server exposes only a narrow explicit subset
- object and relational APIs are explicit, not reflective or inferred
