---
title = "Architecture"
description = "Layering, ownership boundaries, and module responsibilities across the Nexum engine, facades, query layer, and server."
layout = "systems"
eyebrow = "System Design"
lead = "Nexum keeps persistence, indexing, recovery, and schema concerns inside one native core, then layers graph, relational, object, vector, and query surfaces over that shared engine."
metricLabel = "Architecture Goal"
metricValue = "One fast storage spine"
metricCopy = "The design avoids duplicate persistence stacks so optimizations land once and benefit every facade."
---

# Architecture

Status: active architecture

Nexum is organized under `biz.digitalindustry.db.*`.

Repository: [github.com/xgeoff/nexum](https://github.com/xgeoff/nexum)

At a high level:

1. One native storage core owns persistence, recovery, schema primitives, and indexes.
2. Graph, relational, object, and vector capabilities are layered over that shared core.
3. Query providers live in the product layer and target those façades.
4. The built-in server is only a transport layer over the lib-owned providers and maintenance APIs.

## Layering

Dependency direction:

- `db.engine` -> storage internals
- `db.graph|relational|object` -> shared storage engine
- `db.vector` -> shared model, schema, index primitives, and vector facade
- `db.query.cypher` -> graph facade
- `db.query.sql|vector` -> relational facade
- `db.server` -> query providers and service wrappers

The server does not own query semantics.

## Storage Core

Primary engine/runtime classes:

- [`NativeStorageEngine.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/db/engine/NativeStorageEngine.java)
- [`PageBackedRecordStore.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/db/engine/record/PageBackedRecordStore.java)
- [`NativeIndexStore.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/db/engine/NativeIndexStore.java)
- [`PageFile.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/db/engine/page/PageFile.java)
- [`WriteAheadLog.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/db/engine/log/WriteAheadLog.java)

Core responsibilities:

- record-oriented durable storage
- WAL-backed commit/recovery
- persisted engine schema metadata
- exact-match, ordered/range, and vector indexes
- checkpointing and WAL compaction
- file-backed and true memory-only modes

## Product Modules

Graph:

- [`GraphStore.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/db/graph/api/GraphStore.java)
- [`NativeGraphStore.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/db/graph/runtime/NativeGraphStore.java)

Relational:

- [`RelationalStore.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/db/relational/api/RelationalStore.java)
- [`TableDefinition.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/db/relational/api/TableDefinition.java)
- [`NativeRelationalStore.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/db/relational/runtime/NativeRelationalStore.java)

Object:

- [`ObjectStore.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/db/object/api/ObjectStore.java)
- [`ObjectType.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/db/object/api/ObjectType.java)
- [`NativeObjectStore.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/db/object/runtime/NativeObjectStore.java)

Vector:

- [`Vector.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/db/model/Vector.java)
- [`VectorValue.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/db/model/VectorValue.java)
- [`VectorIndex.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/db/index/VectorIndex.java)
- [`FlatVectorIndex.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/db/vector/FlatVectorIndex.java)
- [`VectorQueryProvider.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/db/query/vector/VectorQueryProvider.java)

Each module uses shared storage primitives instead of owning separate persistence implementations.

Cross-facade retrieval is not supported by default. Data written through the graph facade remains graph-shaped, data written through the relational facade remains row-shaped, and data written through the object facade remains object-shaped. That separation is intentional: each facade defines its own `EntityType`, reserved identity fields, indexing rules, and API-level invariants, so Nexum keeps the shared engine unified at the storage layer without pretending the higher-level models are interchangeable automatically.

If an embedder wants cross-facade projection anyway, they can build a custom bridge on top of the shared engine and schema layer, but that mapping is application-defined rather than built in.

## Query Layer

Query providers are lib-owned:

- [`QueryProvider.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/db/query/QueryProvider.java)
- [`QueryProviderRegistry.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/db/query/QueryProviderRegistry.java)
- [`CypherQueryProvider.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/db/query/cypher/CypherQueryProvider.java)
- [`SqlQueryProvider.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/db/query/sql/SqlQueryProvider.java)
- [`VectorQueryProvider.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/db/query/vector/VectorQueryProvider.java)

Current mapping:

- Cypher -> graph facade
- SQL -> relational facade
- Vector -> relational vector indexes

This lets embedders choose between:

- using façades directly
- using query providers directly
- building their own language layer

## Server Layer

The built-in Micronaut server is transport and service wiring only:

- [`Application.java`](https://github.com/xgeoff/nexum/blob/main/server/src/main/biz/digitalindustry/db/server/Application.java)
- [`QueryController.java`](https://github.com/xgeoff/nexum/blob/main/server/src/main/biz/digitalindustry/db/server/controller/QueryController.java)
- [`MaintenanceController.java`](https://github.com/xgeoff/nexum/blob/main/server/src/main/biz/digitalindustry/db/server/controller/MaintenanceController.java)
- [`QueryProviderFactory.java`](https://github.com/xgeoff/nexum/blob/main/server/src/main/biz/digitalindustry/db/server/service/QueryProviderFactory.java)

It exposes:

- `POST /query`
- `GET /admin/maintenance`
- `POST /admin/maintenance/checkpoint`

It does not define Cypher or SQL semantics itself.

## Concurrency Model

Nexum currently standardizes on single-writer/multi-reader access.

- concurrent readers are allowed
- writes are exclusive
- readers may overlap with the single writer
- readers see only last committed state while the writer uses a private overlay
- write-path checkpointing is operator-controlled rather than automatically blocking commits

See [`concurrency-model.md`](/concurrency-model.html).

## Schema Model

The engine persists schema metadata in native engine state.

- graph/object/relational/vector modules register explicit schema definitions
- vector fields and vector indexes are stored as engine schema metadata too
- facade-neutral `SchemaDefinition` entries are reconstructed into facade-specific definitions on reopen
- no separate relational schema file is required

## Current Limits

- query providers are pattern-driven, not full parsers
- SQL is single-table and narrow, not full ANSI SQL
- concurrency is SWMR, not MVCC or multi-writer
- some native index structural delete paths still fall back to rebuild behavior
