# Nexum

Nexum is a native embedded multi-model database implemented under `biz.digitalindustry.*`.

The repository now has two active modules:

- `app`: native storage engine plus graph, object, and relational facades
- `server`: Micronaut HTTP API over the native graph facade

## Current Architecture

Core storage:

- [`NativeStorageEngine.java`](app/src/main/java/biz/digitalindustry/storage/engine/NativeStorageEngine.java)
- [`PageBackedRecordStore.java`](app/src/main/java/biz/digitalindustry/storage/store/PageBackedRecordStore.java)
- [`NativeIndexStore.java`](app/src/main/java/biz/digitalindustry/storage/engine/NativeIndexStore.java)
- [`PageFile.java`](app/src/main/java/biz/digitalindustry/storage/page/PageFile.java)
- [`WriteAheadLog.java`](app/src/main/java/biz/digitalindustry/storage/log/WriteAheadLog.java)

Facades:

- graph: [`NativeGraphStore.java`](app/src/main/java/biz/digitalindustry/graph/engine/NativeGraphStore.java)
- object: [`NativeObjectStore.java`](app/src/main/java/biz/digitalindustry/object/engine/NativeObjectStore.java)
- relational: [`NativeRelationalStore.java`](app/src/main/java/biz/digitalindustry/relational/engine/NativeRelationalStore.java)

Server entry points:

- [`Application.java`](server/src/main/biz/digitalindustry/db/server/Application.java)
- [`QueryController.java`](server/src/main/biz/digitalindustry/db/server/controller/QueryController.java)
- [`GraphStore.java`](server/src/main/biz/digitalindustry/db/server/service/GraphStore.java)

## What Works

The native engine currently supports:

- durable page-backed record storage
- transactions with WAL-backed recovery
- exact-match and ordered/range indexes
- native persisted index pages with recursive directory lookup
- graph CRUD and traversal
- object CRUD with explicit codecs and references
- relational CRUD with exact and range lookup

The HTTP API currently supports a tested narrow query subset for graph operations through `POST /query`.

## Running

Run the full test suite:

```bash
./gradlew test
```

Run the server:

```bash
./gradlew :server:run -Dgraph.db.path=./data/nexum-graph.dbs
```

If `graph.db.path` is omitted, the server uses a temporary native database file.

## Query API

Request shape:

```json
{ "cypher": "MATCH (n) RETURN n" }
```

or:

```json
{ "sql": "SELECT * FROM nodes" }
```

Supported Cypher-like operations include:

- node create/read/update/delete
- edge create/read/update/delete
- outgoing traversal

Supported SQL-like operations include:

- `INSERT INTO nodes`
- `INSERT INTO edges`
- `SELECT * FROM nodes`
- `SELECT * FROM edges`
- exact-match node and edge filters
- update/delete by key fields

See [`server-query-guide.md`](docs/server-query-guide.md) for the exact supported syntax.

## Docs

- [`unified-engine-model.md`](docs/unified-engine-model.md)
- [`object-facade.md`](docs/object-facade.md)
- [`relational-facade.md`](docs/relational-facade.md)
- [`server-query-guide.md`](docs/server-query-guide.md)

## Current Limits

- the server query layer is pattern-driven, not a full parser
- graph traversal syntax is still narrow
- object and relational facades are explicit APIs, not reflection-based frameworks
- the native index tree still has some rebuild fallbacks for less common structural delete cases
