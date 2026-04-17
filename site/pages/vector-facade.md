---
title = "Vector Facade"
description = "Vector fields, vector indexes, and nearest-neighbor lookup over the shared native engine."
layout = "reference"
eyebrow = "API Reference"
lead = "Vector support is not a separate storage stack. Nexum treats vectors as first-class schema and index primitives inside the same engine used by the graph, relational, and object facades."
statusLabel = "Vector Scope"
accent = "Nearest-neighbor over one engine"
---

# Vector Facade

Status: active native implementation

Vector support in Nexum is implemented horizontally across the core model, schema, indexing, and query layers.

It is not a standalone database inside the database. The important design choice is that vectors are first-class engine data and index primitives, so vector search composes with the same persistence, recovery, and schema system used everywhere else.

The schema registry is now core-owned and facade-neutral. Relational tables, object types, graph records, and vector collections all register explicit `SchemaDefinition` entries with a `SchemaKind`, then each facade reconstructs its own higher-level contract from that shared metadata on reopen.

Source packages:

- [github.com/xgeoff/nexum/tree/main/lib/src/main/java/biz/digitalindustry/db/vector](https://github.com/xgeoff/nexum/tree/main/lib/src/main/java/biz/digitalindustry/db/vector)
- [github.com/xgeoff/nexum/tree/main/lib/src/main/java/biz/digitalindustry/db/index](https://github.com/xgeoff/nexum/tree/main/lib/src/main/java/biz/digitalindustry/db/index)
- [github.com/xgeoff/nexum/tree/main/lib/src/main/java/biz/digitalindustry/db/query/vector](https://github.com/xgeoff/nexum/tree/main/lib/src/main/java/biz/digitalindustry/db/query/vector)

## Key Types

- [`Vector.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/db/model/Vector.java)
- [`VectorValue.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/db/model/VectorValue.java)
- [`FieldDefinition.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/db/schema/FieldDefinition.java)
- [`IndexDefinition.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/db/schema/IndexDefinition.java)
- [`SchemaDefinition.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/db/schema/SchemaDefinition.java)
- [`VectorIndex.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/db/index/VectorIndex.java)
- [`VectorMatch.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/db/index/VectorMatch.java)
- [`FlatVectorIndex.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/db/vector/FlatVectorIndex.java)
- [`Distances.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/db/vector/Distances.java)
- [`VectorIndexDefinition.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/db/vector/VectorIndexDefinition.java)
- [`VectorCollectionDefinition.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/db/vector/api/VectorCollectionDefinition.java)
- [`NativeVectorStore.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/db/vector/runtime/NativeVectorStore.java)
- [`VectorQueryProvider.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/db/query/vector/VectorQueryProvider.java)

## Design

Nexum introduces vector support in four places at once:

- the model layer gets `Vector` and `VectorValue`
- the schema layer gets vector field metadata with fixed dimension
- the index layer gets vector nearest-neighbor lookup
- the query layer gets a dedicated vector provider

That means vector search is part of the same engine contract as any other index type.

## Schema Rules

Vector fields are explicit schema, not dynamic payloads.

- a vector field must declare a fixed positive dimension
- dimension consistency is enforced at schema and write time
- vector indexes are tied to a single field
- a vector index must declare both dimension and distance metric

Current distance metrics:

- `euclidean`
- `cosine`

## Vector Collection API

The vector facade now exposes a dedicated collection API on top of the shared engine:

- `VectorCollectionDefinition` defines the collection key field, vector field, dimension, distance metric, and any additional fields
- `NativeVectorStore` persists and restores vector collections directly from shared schema metadata
- `VectorDocument` represents one stored vector item
- `VectorDocumentMatch` returns nearest-neighbor hits with distance

Under the hood this still uses the same engine-owned schema and vector index primitives, but callers no longer need to model embeddings as relational tables unless they explicitly want SQL-style row access too.

## Example

```java
import biz.digitalindustry.db.model.StringValue;
import biz.digitalindustry.db.model.Vector;
import biz.digitalindustry.db.model.VectorValue;
import biz.digitalindustry.db.schema.FieldDefinition;
import biz.digitalindustry.db.schema.ValueType;
import biz.digitalindustry.db.vector.api.VectorCollectionDefinition;
import biz.digitalindustry.db.vector.api.VectorDocument;
import biz.digitalindustry.db.vector.runtime.NativeVectorStore;

import java.util.List;
import java.util.Map;

VectorCollectionDefinition embeddings = new VectorCollectionDefinition(
        "embeddings",
        "id",
        "embedding",
        3,
        "euclidean",
        List.of(
                new FieldDefinition("id", ValueType.STRING, true, false),
                FieldDefinition.vector("embedding", 3, true),
                new FieldDefinition("label", ValueType.STRING, true, false)
        ),
        List.of()
);

try (var store = NativeVectorStore.fileBacked("./data/embeddings.dbs")) {
    store.registerCollection(embeddings);

    store.upsert(embeddings, new VectorDocument("e1", Map.of(
            "id", new StringValue("e1"),
            "label", new StringValue("alpha"),
            "embedding", new VectorValue(Vector.of(1.0f, 0.0f, 0.0f))
    )));

    var matches = store.nearest(
            embeddings,
            Vector.of(1.0f, 0.0f, 0.0f),
            5
    );
}
```

## Relational Integration

If you want SQL and vector search over the same physical data, the relational facade still supports vector columns and nearest-neighbor lookup through `RelationalStore.findNearestBy(...)`.

That is an integration path, not the ownership boundary. Vector schema is now a first-class engine concern and the vector facade is a first-class API surface.

## Provider Model

Vector search also fits the existing provider model.

Use the lib-owned `VectorQueryProvider` when you want either the text form or the structured JSON form without bringing in the server.

```java
import biz.digitalindustry.db.query.QueryCommand;
import biz.digitalindustry.db.query.vector.VectorQueryProvider;
import biz.digitalindustry.db.vector.runtime.NativeVectorStore;

var vectorStore = NativeVectorStore.fileBacked("./data/embeddings.dbs");
var provider = new VectorQueryProvider(vectorStore);

var result = provider.execute(new QueryCommand("vector", Map.of(
        "from", "embeddings",
        "vector", Map.of(
                "field", "embedding",
                "nearest", Map.of(
                        "vector", List.of(1.0, 0.0, 0.0),
                        "k", 3,
                        "distance", "euclidean"
                )
        )
)));
```

Supported vector-json shape:

- `from`: vector collection name
- `vector.field`: configured vector field name for that collection
- `vector.nearest.vector`: numeric array
- `vector.nearest.k`: max result count
- `vector.nearest.distance`: distance metric and it must match the configured vector index

Canonical vector-json request:

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

Canonical vector-text request:

```text
VECTOR FROM embeddings
FIELD embedding
NEAREST [1.0, 0.0, 0.0]
K 3
DISTANCE euclidean
```

The same vector-json and vector-text forms both work through the built-in Micronaut server at `POST /query`.

## Current Limits

- the current implementation uses a flat in-memory nearest-neighbor index, not HNSW or IVF
- the query provider still resolves vector lookups through the relational query surface today
- vector-aware composition with graph traversal or object queries is not exposed as a higher-level planner yet
