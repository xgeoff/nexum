# Nexum

Nexum is a native embedded multi-model database implemented under `biz.digitalindustry.db.*`.

The repository now has three active modules:

- `lib`: native storage engine plus graph, object, relational, and query modules
- `jackson`: optional Jackson DTO adapter over the lib-owned object facade
- `server`: Micronaut HTTP transport over the lib-owned query and maintenance layers

## Current Architecture

The canonical architecture page is [`architecture.md`](site/pages/architecture.md).

Core storage:

- [`NativeStorageEngine.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/db/engine/NativeStorageEngine.java)
- [`PageBackedRecordStore.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/db/engine/record/PageBackedRecordStore.java)
- [`NativeIndexStore.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/db/engine/NativeIndexStore.java)
- [`PageFile.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/db/engine/page/PageFile.java)
- [`WriteAheadLog.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/db/engine/log/WriteAheadLog.java)

Facades:

- graph: [`NativeGraphStore.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/db/graph/runtime/NativeGraphStore.java)
- object: [`NativeObjectStore.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/db/object/runtime/NativeObjectStore.java)
- relational: [`NativeRelationalStore.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/db/relational/runtime/NativeRelationalStore.java)
- vector: [`NativeVectorStore.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/db/vector/runtime/NativeVectorStore.java)

Query providers:

- Cypher: [`CypherQueryProvider.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/db/query/cypher/CypherQueryProvider.java)
- SQL: [`SqlQueryProvider.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/db/query/sql/SqlQueryProvider.java)
- vector: [`VectorQueryProvider.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/db/query/vector/VectorQueryProvider.java)
- shared registry: [`QueryProviderRegistry.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/db/query/QueryProviderRegistry.java)

Server entry points:

- [`Application.java`](https://github.com/xgeoff/nexum/blob/main/server/src/main/biz/digitalindustry/db/server/Application.java)
- [`QueryController.java`](https://github.com/xgeoff/nexum/blob/main/server/src/main/biz/digitalindustry/db/server/controller/QueryController.java)
- [`GraphStore.java`](https://github.com/xgeoff/nexum/blob/main/server/src/main/biz/digitalindustry/db/server/service/GraphStore.java)
- [`RelationalStoreService.java`](https://github.com/xgeoff/nexum/blob/main/server/src/main/biz/digitalindustry/db/server/service/RelationalStoreService.java)

## What Works

The native engine currently supports:

- durable page-backed record storage
- transactions with WAL-backed recovery
- exact-match, ordered/range, and vector indexes
- native persisted index pages with recursive directory lookup
- graph CRUD and traversal
- object CRUD with explicit codecs and references
- relational CRUD with exact and range lookup
- dedicated vector collection facade with nearest-neighbor lookup
- a documented single-writer/multi-reader access model

The HTTP API currently supports a tested narrow query subset through `POST /query`.
Cypher is graph-backed, SQL is relational-backed, and vector queries are vector-facade-backed nearest-neighbor lookups.
It is still an experimental query surface, not yet a general database language layer.

## Running

Run the full test suite:

```bash
./gradlew test
```

Run the lightweight benchmark harness:

```bash
./gradlew :lib:benchmark
```

Run the larger-scale benchmark profile:

```bash
./gradlew :lib:benchmark -DbenchmarkProfile=large
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
- `vector.db.mode=file|memory`
- `vector.db.path=./data/nexum-vector.dbs`
- `vector.db.page-size=8192`
- `vector.db.max-wal-bytes=536870912`
- `vector.db.checkpoint-on-close=true`

The benchmark harness now uses explicit files under `build/benchmarks/` and cleans them up after each run.

Embedded factory examples:

```java
var graph = biz.digitalindustry.db.graph.runtime.NativeGraphStore.fileBacked("./data/app-graph.dbs");
var graphMemory = biz.digitalindustry.db.graph.runtime.NativeGraphStore.memoryOnly();

var relational = biz.digitalindustry.db.relational.runtime.NativeRelationalStore.fileBacked("./data/app-relational.dbs");
var relationalMemory = biz.digitalindustry.db.relational.runtime.NativeRelationalStore.memoryOnly();

var objectStore = biz.digitalindustry.db.object.runtime.NativeObjectStore.fileBacked("./data/app-object.dbs");
var objectMemory = biz.digitalindustry.db.object.runtime.NativeObjectStore.memoryOnly();

var vectorStore = biz.digitalindustry.db.vector.runtime.NativeVectorStore.fileBacked("./data/app-vector.dbs");
var vectorMemory = biz.digitalindustry.db.vector.runtime.NativeVectorStore.memoryOnly();
```

Optional Jackson adapter example:

```java
var jackson = new biz.digitalindustry.db.jackson.JacksonAdapter("./data/app-object.dbs");
jackson.register(PersonDto.class, "id");
```

Embedded query-provider examples:

```java
var cypher = new biz.digitalindustry.db.query.cypher.CypherQueryProvider(graph);
var sql = new biz.digitalindustry.db.query.sql.SqlQueryProvider(relational);
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

or:

```json
{
  "vector": {
    "from": "embeddings",
    "vector": {
      "field": "embedding",
      "nearest": {
        "vector": [1.0, 0.0, 0.0],
        "k": 3,
        "distance": "euclidean"
      }
    }
  }
}
```

or:

```text
VECTOR FROM embeddings
FIELD embedding
NEAREST [1.0, 0.0, 0.0]
K 3
DISTANCE euclidean
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

- [`getting-started.md`](site/pages/getting-started.md)
- [`object-query-api-design.md`](site/pages/object-query-api-design.md)
- [`architecture.md`](site/pages/architecture.md)
- [`unified-engine-model.md`](site/pages/unified-engine-model.md)
- [`concurrency-model.md`](site/pages/concurrency-model.md)
- [`object-facade.md`](site/pages/object-facade.md)
- [`jackson-adapter.md`](site/pages/jackson-adapter.md)
- [`relational-facade.md`](site/pages/relational-facade.md)
- [`vector-facade.md`](site/pages/vector-facade.md)
- [`server-query-guide.md`](site/pages/server-query-guide.md)
- [`mcp-server.md`](site/pages/mcp-server.md)

## Current Limits

- the query layer is pattern-driven, not a full parser
- the server is a transport wrapper over lib-owned query providers
- graph traversal syntax is still narrow
- object and relational facades are explicit APIs, not reflection-based frameworks
- the native index tree still has some rebuild fallbacks for less common structural delete cases
- concurrency is intentionally single-writer/multi-reader, not MVCC or multi-writer
- readers see last committed state only while the single writer works in a private overlay
