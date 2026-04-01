# Server Query Guide

This document describes the current native storage-backed query subset implemented by the Micronaut server.

Primary entry point:

- `POST /query`

Configuration:

- `graph.db.path=/path/to/database.dbs`

The server now runs only on the native graph backend.

## Request Shape

Cypher-like:

```json
{ "cypher": "MATCH (n) RETURN n" }
```

SQL-like:

```json
{ "sql": "SELECT * FROM nodes" }
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

Create:

```sql
INSERT INTO nodes (id, label) VALUES ('alice', 'Person')
INSERT INTO edges (from_id, to_id, type, weight) VALUES ('alice', 'bob', 'KNOWS', 1.75)
```

Read:

```sql
SELECT * FROM nodes
SELECT * FROM nodes WHERE id = 'alice'
SELECT * FROM nodes WHERE label = 'Person'
SELECT * FROM nodes WHERE outgoing_from = 'alice'
SELECT * FROM nodes WHERE outgoing_from = 'alice' AND edge_type = 'KNOWS'
SELECT * FROM edges
SELECT * FROM edges WHERE from_id = 'alice'
SELECT * FROM edges WHERE to_id = 'bob'
SELECT * FROM edges WHERE type = 'KNOWS'
```

Update:

```sql
UPDATE nodes SET label = 'Architect' WHERE id = 'alice'
UPDATE edges SET weight = 9.25 WHERE from_id = 'alice' AND to_id = 'bob' AND type = 'KNOWS'
```

Delete:

```sql
DELETE FROM nodes WHERE id = 'alice'
DELETE FROM edges WHERE from_id = 'alice' AND to_id = 'bob' AND type = 'KNOWS'
```

## Response Notes

Node rows include:

- `entityType=node`
- `label`
- `outgoingCount`
- `incomingCount`

Edge rows include:

- `entityType=edge`
- `fromId`
- `toId`
- `type`
- `weight`

## Constraints

- the syntax is pattern-based, not a general parser
- traversal support is currently outgoing-only at the query layer
- edge uniqueness is not enforced
