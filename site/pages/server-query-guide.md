---
title = "Server Query Guide"
description = "The current Cypher-like and SQL-like subset exposed by the Nexum Micronaut transport layer."
layout = "query"
eyebrow = "Transport Guide"
lead = "The built-in server is a narrow HTTP surface over lib-owned query providers. Cypher routes into the graph facade, SQL routes into the relational facade, and vector queries follow the same provider-dispatch pattern in either text or structured JSON form."
---

# Server Query Guide

This document describes the current native storage-backed query subset implemented by the Micronaut server.

The server is only the HTTP transport layer. Query semantics live in the lib module under `biz.digitalindustry.db.query.*`.

Primary entry point:

- `POST /query`

Configuration:

- `graph.db.path=/path/to/database.dbs`
- `relational.db.path=/path/to/relational.dbs`
- `vector.db.path=/path/to/vector.dbs`

The server routes:

- `cypher` queries to the graph facade through `CypherQueryProvider`
- `sql` queries to the relational facade through `SqlQueryProvider`
- `vector` queries to the vector facade through `VectorQueryProvider`

See [`object-query-api-design.md`](/object-query-api-design.html) for the proposed evolution of this provider model toward structured JSON payloads and object operations.

## Request Shape

Cypher-like:

```json
{ "cypher": "MATCH (n) RETURN n" }
```

SQL-like:

```json
{ "sql": "SELECT * FROM users" }
```

Vector:

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

Vector text:

```text
VECTOR FROM embeddings
FIELD embedding
NEAREST [1.0, 0.0, 0.0]
K 3
DISTANCE euclidean
```

## Supported Cypher-like Operations

Create a node:

```cypher
CREATE (n:Person {id:'alice'})
```

Create an edge and upsert endpoints:

```cypher
CREATE (a:Person {id:'alice'})-[:KNOWS {weight:2.5}]->(b:Person {id:'bob'})
```

Read nodes:

```cypher
MATCH (n) RETURN n
MATCH (n {id:'alice'}) RETURN n
MATCH (n:Person) RETURN n
```

Traverse outgoing neighbors:

```cypher
MATCH (a {id:'alice'})-[:KNOWS]->(b) RETURN b
MATCH (a {id:'alice'})-[]->(b) RETURN b
```

Read edges:

```cypher
MATCH (a {id:'alice'})-[:KNOWS]->(b) RETURN edge
MATCH (a)-[]->(b) RETURN edge
```

Update:

```cypher
MATCH (n {id:'alice'}) SET n:Engineer
MATCH (a {id:'alice'})-[r:KNOWS]->(b {id:'bob'}) SET r.weight = 4.5
```

Delete:

```cypher
MATCH (a {id:'alice'})-[r:KNOWS]->(b {id:'bob'}) DELETE r
MATCH (n {id:'bob'}) DELETE n
```

## Supported SQL-like Operations

Create schema:

```sql
CREATE TABLE users (id STRING PRIMARY KEY, name STRING NOT NULL, age LONG NOT NULL, active BOOLEAN NOT NULL)
CREATE INDEX users_name_idx ON users (name)
```

Insert:

```sql
INSERT INTO users (id, name, age, active) VALUES ('u1', 'Ada', 37, true)
```

Read:

```sql
SELECT * FROM users
SELECT * FROM users WHERE id = 'u1'
SELECT * FROM users WHERE name = 'Ada'
SELECT * FROM users WHERE age BETWEEN 30 AND 40
```

Update:

```sql
UPDATE users SET name = 'Ada Lovelace', active = false WHERE id = 'u1'
```

Delete:

```sql
DELETE FROM users WHERE id = 'u1'
```

## Supported Vector Operations

Nearest-neighbor lookup:

```json
{
  "vector": {
    "from": "embeddings",
    "vector": {
      "field": "embedding",
      "nearest": {
        "vector": [1.0, 0.0, 0.0],
        "k": 5,
        "distance": "euclidean"
      }
    }
  }
}
```

Payload rules:

- `from` must name an existing relational table
- `vector.field` must name a vector-indexed column on that table
- `vector.nearest.vector` must be a numeric array with the schema-declared dimension
- `vector.nearest.k` must be a positive integer
- `vector.nearest.distance` must match the declared distance metric on the target vector index

Text rules:

- first line: `VECTOR FROM <source>`
- second line: `FIELD <field-name>`
- third line: `NEAREST [<number>, <number>, ...]`
- fourth line: `K <integer>`
- fifth line: `DISTANCE <metric>`

## Response Notes

Row results include:

- `entityType=row`
- user-defined column properties
- row id mapped to the relational primary key value

Vector results also include:

- `distance`

## Constraints

- the syntax is pattern-based, not a general parser
- SQL support is currently single-table only
- SQL schema definitions are narrow and explicit
- relational schema is restored from persisted engine schema on reopen
- vector search expects tables and vector indexes to be defined through the relational API today rather than the SQL subset
