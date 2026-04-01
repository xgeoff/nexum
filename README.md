# Nexum

Nexum is a native embedded multi-model database implemented under `biz.digitalindustry.storage.*`.

The repository now has two active modules:

- `app`: native storage engine plus graph, object, relational, and query modules
- `server`: Micronaut HTTP transport over the app-owned query and maintenance layers

## Current Architecture

The canonical architecture page is [`architecture.md`](site/pages/architecture.md).

Core storage:

- [`NativeStorageEngine.java`](app/src/main/java/biz/digitalindustry/storage/engine/NativeStorageEngine.java)
- [`PageBackedRecordStore.java`](app/src/main/java/biz/digitalindustry/storage/store/PageBackedRecordStore.java)
- [`NativeIndexStore.java`](app/src/main/java/biz/digitalindustry/storage/engine/NativeIndexStore.java)
- [`PageFile.java`](app/src/main/java/biz/digitalindustry/storage/page/PageFile.java)
- [`WriteAheadLog.java`](app/src/main/java/biz/digitalindustry/storage/log/WriteAheadLog.java)

Facades:

- graph: [`NativeGraphStore.java`](app/src/main/java/biz/digitalindustry/storage/graph/engine/NativeGraphStore.java)
- object: [`NativeObjectStore.java`](app/src/main/java/biz/digitalindustry/storage/object/engine/NativeObjectStore.java)
- relational: [`NativeRelationalStore.java`](app/src/main/java/biz/digitalindustry/storage/relational/engine/NativeRelationalStore.java)

Query providers:

- Cypher: [`CypherQueryProvider.java`](app/src/main/java/biz/digitalindustry/storage/query/cypher/CypherQueryProvider.java)
- SQL: [`SqlQueryProvider.java`](app/src/main/java/biz/digitalindustry/storage/query/sql/SqlQueryProvider.java)
- shared registry: [`QueryProviderRegistry.java`](app/src/main/java/biz/digitalindustry/storage/query/QueryProviderRegistry.java)

Server entry points:

- [`Application.java`](server/src/main/biz/digitalindustry/storage/server/Application.java)
- [`QueryController.java`](server/src/main/biz/digitalindustry/storage/server/controller/QueryController.java)
- [`GraphStore.java`](server/src/main/biz/digitalindustry/storage/server/service/GraphStore.java)
- [`RelationalStoreService.java`](server/src/main/biz/digitalindustry/storage/server/service/RelationalStoreService.java)

## What Works

The native engine currently supports:

- durable page-backed record storage
- transactions with WAL-backed recovery
- exact-match and ordered/range indexes
- native persisted index pages with recursive directory lookup
- graph CRUD and traversal
- object CRUD with explicit codecs and references
- relational CRUD with exact and range lookup
- a documented single-writer/multi-reader access model

The HTTP API currently supports a tested narrow query subset through `POST /query`.
Cypher is graph-backed and SQL is relational-backed.
It is still an experimental query surface, not yet a general database language layer.

## Running

Run the full test suite:

```bash
./gradlew test
```

Run the lightweight benchmark harness:

```bash
./gradlew :app:benchmark
```

Run the larger-scale benchmark profile:

```bash
./gradlew :app:benchmark -DbenchmarkProfile=large
```

Run the server:

```bash
./gradlew :server:run -Dgraph.db.path=./data/nexum-graph.dbs
```

If `graph.db.path` is omitted, the server uses `./data/nexum-graph.dbs`.

Run the server in true memory-only mode:

```bash
./gradlew :server:run -Dgraph.db.mode=memory
```

Server storage properties:

- `graph.db.mode=file|memory`
- `graph.db.path=./data/nexum-graph.dbs`
- `graph.db.page-size=8192`
- `graph.db.max-wal-bytes=536870912`
- `graph.db.checkpoint-on-close=true`
- `relational.db.mode=file|memory`
- `relational.db.path=./data/nexum-relational.dbs`
- `relational.db.page-size=8192`
- `relational.db.max-wal-bytes=536870912`
- `relational.db.checkpoint-on-close=true`

The benchmark harness now uses explicit files under `build/benchmarks/` and cleans them up after each run.

Embedded factory examples:

```java
var graph = biz.digitalindustry.storage.graph.engine.NativeGraphStore.fileBacked("./data/app-graph.dbs");
var graphMemory = biz.digitalindustry.storage.graph.engine.NativeGraphStore.memoryOnly();

var relational = biz.digitalindustry.storage.relational.engine.NativeRelationalStore.fileBacked("./data/app-relational.dbs");
var relationalMemory = biz.digitalindustry.storage.relational.engine.NativeRelationalStore.memoryOnly();

var objectStore = biz.digitalindustry.storage.object.engine.NativeObjectStore.fileBacked("./data/app-object.dbs");
var objectMemory = biz.digitalindustry.storage.object.engine.NativeObjectStore.memoryOnly();
```

Embedded query-provider examples:

```java
var cypher = new biz.digitalindustry.storage.query.cypher.CypherQueryProvider(graph);
var sql = new biz.digitalindustry.storage.query.sql.SqlQueryProvider(relational);
```

## Query API

Request shape:

```json
{ "cypher": "MATCH (n) RETURN n" }
```

or:

```json
{ "sql": "SELECT * FROM users" }
```

Supported Cypher-like operations include:

- node create/read/update/delete
- edge create/read/update/delete
- outgoing traversal

Supported SQL-like operations include:

- `CREATE TABLE`
- `CREATE INDEX`
- `INSERT INTO <table>`
- `SELECT * FROM <table>`
- exact-match and range filters
- `UPDATE ... WHERE ...`
- `DELETE ... WHERE ...`

The SQL endpoint is now relational-backed and pattern-driven. It supports a narrow single-table subset, not full ANSI SQL.

See [`server-query-guide.md`](site/pages/server-query-guide.md) for the exact supported syntax.

## Docs

- [`architecture.md`](site/pages/architecture.md)
- [`unified-engine-model.md`](site/pages/unified-engine-model.md)
- [`concurrency-model.md`](site/pages/concurrency-model.md)
- [`object-facade.md`](site/pages/object-facade.md)
- [`relational-facade.md`](site/pages/relational-facade.md)
- [`server-query-guide.md`](site/pages/server-query-guide.md)

## Current Limits

- the query layer is pattern-driven, not a full parser
- the server is a transport wrapper over app-owned query providers
- graph traversal syntax is still narrow
- object and relational facades are explicit APIs, not reflection-based frameworks
- the native index tree still has some rebuild fallbacks for less common structural delete cases
- concurrency is intentionally single-writer/multi-reader, not MVCC or multi-writer
- readers see last committed state only while the single writer works in a private overlay
