---
title = "Architecture"
description = "Layering, ownership boundaries, and module responsibilities across the Nexum engine, facades, query layer, and server."
layout = "systems"
eyebrow = "System Design"
lead = "Nexum keeps persistence, indexing, recovery, and schema concerns inside one native core, then layers graph, relational, object, and query surfaces over that shared engine."
metricLabel = "Architecture Goal"
metricValue = "One fast storage spine"
metricCopy = "The design avoids duplicate persistence stacks so optimizations land once and benefit every facade."
---

# Architecture

Status: active architecture

Nexum is organized under `biz.digitalindustry.storage.*`.

Repository: [github.com/xgeoff/nexum](https://github.com/xgeoff/nexum)

At a high level:

1. One native storage core owns persistence, recovery, schema primitives, and indexes.
2. Graph, relational, and object modules are façades over that shared core.
3. Query providers live in the product layer and target those façades.
4. The built-in server is only a transport layer over the lib-owned providers and maintenance APIs.

## Layering

Dependency direction:

- `storage.engine` -> storage internals
- `storage.graph|relational|object` -> shared storage engine
- `storage.query.cypher` -> graph facade
- `storage.query.sql` -> relational facade
- `storage.server` -> query providers and service wrappers

The server does not own query semantics.

## Storage Core

Primary engine/runtime classes:

- [`NativeStorageEngine.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/storage/engine/NativeStorageEngine.java)
- [`PageBackedRecordStore.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/storage/store/PageBackedRecordStore.java)
- [`NativeIndexStore.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/storage/engine/NativeIndexStore.java)
- [`PageFile.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/storage/page/PageFile.java)
- [`WriteAheadLog.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/storage/log/WriteAheadLog.java)

Core responsibilities:

- record-oriented durable storage
- WAL-backed commit/recovery
- persisted engine schema metadata
- exact-match and ordered/range indexes
- checkpointing and WAL compaction
- file-backed and true memory-only modes

## Product Modules

Graph:

- [`GraphStore.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/storage/graph/api/GraphStore.java)
- [`NativeGraphStore.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/storage/graph/engine/NativeGraphStore.java)

Relational:

- [`RelationalStore.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/storage/relational/api/RelationalStore.java)
- [`TableDefinition.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/storage/relational/api/TableDefinition.java)
- [`NativeRelationalStore.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/storage/relational/engine/NativeRelationalStore.java)

Object:

- [`ObjectStore.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/storage/object/api/ObjectStore.java)
- [`ObjectType.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/storage/object/api/ObjectType.java)
- [`NativeObjectStore.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/storage/object/engine/NativeObjectStore.java)

Each module uses shared storage primitives instead of owning separate persistence implementations.

Cross-facade retrieval is not supported by default. Data written through the graph facade remains graph-shaped, data written through the relational facade remains row-shaped, and data written through the object facade remains object-shaped. That separation is intentional: each facade defines its own `EntityType`, reserved identity fields, indexing rules, and API-level invariants, so Nexum keeps the shared engine unified at the storage layer without pretending the higher-level models are interchangeable automatically.

If an embedder wants cross-facade projection anyway, they can build a custom bridge on top of the shared engine and schema layer, but that mapping is application-defined rather than built in.

## Query Layer

Query providers are lib-owned:

- [`QueryProvider.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/storage/query/QueryProvider.java)
- [`QueryProviderRegistry.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/storage/query/QueryProviderRegistry.java)
- [`CypherQueryProvider.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/storage/query/cypher/CypherQueryProvider.java)
- [`SqlQueryProvider.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/storage/query/sql/SqlQueryProvider.java)

Current mapping:

- Cypher -> graph facade
- SQL -> relational facade

This lets embedders choose between:

- using façades directly
- using query providers directly
- building their own language layer

## Server Layer

The built-in Micronaut server is transport and service wiring only:

- [`Application.java`](https://github.com/xgeoff/nexum/blob/main/server/src/main/biz/digitalindustry/storage/server/Application.java)
- [`QueryController.java`](https://github.com/xgeoff/nexum/blob/main/server/src/main/biz/digitalindustry/storage/server/controller/QueryController.java)
- [`MaintenanceController.java`](https://github.com/xgeoff/nexum/blob/main/server/src/main/biz/digitalindustry/storage/server/controller/MaintenanceController.java)
- [`QueryProviderFactory.java`](https://github.com/xgeoff/nexum/blob/main/server/src/main/biz/digitalindustry/storage/server/service/QueryProviderFactory.java)

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

- graph/object/relational modules register explicit schema definitions
- relational tables are reconstructed from persisted engine schema on reopen
- no separate relational schema file is required

## Current Limits

- query providers are pattern-driven, not full parsers
- SQL is single-table and narrow, not full ANSI SQL
- concurrency is SWMR, not MVCC or multi-writer
- some native index structural delete paths still fall back to rebuild behavior
