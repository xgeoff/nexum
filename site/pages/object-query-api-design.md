---
title = "Object Query API Design"
description = "Proposed evolution of the unified /query endpoint to support object operations through structured JSON payloads while preserving the provider model."
layout = "query"
eyebrow = "API Design"
lead = "Keep the elegant provider-dispatch model, but evolve provider payloads from plain strings to JSON objects so object storage can be exposed cleanly through the same /query endpoint."
---

# Object Query API Design

Status: proposed server API evolution

Repository: [github.com/xgeoff/nexum](https://github.com/xgeoff/nexum)

This document proposes how Nexum can add object operations to the server without abandoning the current provider model.

## Goals

- preserve the single `POST /query` entry point
- preserve provider dispatch by top-level key
- retain compatibility with existing `sql` and `cypher` callers
- support object operations with structured JSON rather than string DSLs
- keep the API narrow, explicit, and compatible with the current typed object facade

## Current Shape

Today the request shape is:

```json
{ "sql": "SELECT * FROM users" }
```

or:

```json
{ "cypher": "MATCH (n) RETURN n" }
```

That is elegant, but too restrictive for object storage because object operations need structured payloads for actions such as type registration, document writes, and selectors.

## Proposed Shape

The top-level request remains a single-entry object:

```json
{ "<provider>": <provider-payload> }
```

The provider payload becomes:

- either a string, for backward compatibility
- or a JSON object, which becomes the canonical shape going forward

Canonical examples:

```json
{ "sql": { "queryText": "SELECT * FROM users" } }
```

```json
{ "cypher": { "queryText": "MATCH (n) RETURN n" } }
```

```json
{
  "object": {
    "action": "get",
    "type": "person",
    "key": "person-1"
  }
}
```

## Backward Compatibility

The server should continue accepting the current string form:

```json
{ "sql": "SELECT * FROM users" }
```

and normalize it internally to:

```json
{ "sql": { "queryText": "SELECT * FROM users" } }
```

The same rule applies to `cypher`.

This gives Nexum:

- no immediate break for existing clients
- one canonical internal request model
- room for richer provider payloads later

## Normalization Rules

For every request:

1. the JSON body must contain exactly one top-level key
2. the key is the provider name, such as `sql`, `cypher`, or `object`
3. if the value is a string, normalize it to `{ "queryText": "<value>" }`
4. if the value is an object, pass it through as provider payload
5. any other payload type is a `400 Bad Request`

Example invalid requests:

```json
{ "sql": 42 }
```

```json
{ "sql": { "queryText": "SELECT * FROM users" }, "cypher": { "queryText": "MATCH (n) RETURN n" } }
```

## Provider Contract

The current `QueryProvider` contract is string-based. To support normalized provider payloads cleanly, the provider boundary should evolve from:

```java
String queryType();
QueryResult execute(String query);
```

to something like:

```java
String queryType();
QueryResult execute(QueryCommand command);
```

Where `QueryCommand` represents:

- provider name
- normalized payload object

Conceptually:

```java
public record QueryCommand(
        String queryType,
        Map<String, Object> payload
) {}
```

That keeps the registry/provider architecture intact while making payloads extensible.

## SQL Provider Payload

First version:

```json
{
  "sql": {
    "queryText": "SELECT * FROM users"
  }
}
```

Possible future extension:

```json
{
  "sql": {
    "queryText": "SELECT * FROM users WHERE id = ?",
    "params": ["u1"]
  }
}
```

For now, only `queryText` is required.

## Cypher Provider Payload

First version:

```json
{
  "cypher": {
    "queryText": "MATCH (n) RETURN n"
  }
}
```

Possible future extension could mirror SQL with additional options, but the first cut should stay narrow.

## Object Provider Payload

The object provider should be JSON-native and action-oriented.

Recommended first actions:

- `registerType`
- `put`
- `get`
- `delete`
- `scan`
- `find`

### Register Type

```json
{
  "object": {
    "action": "registerType",
    "type": "person",
    "definition": {
      "keyField": "id",
      "fields": {
        "id": { "type": "string", "required": true },
        "name": { "type": "string", "required": true },
        "age": { "type": "long", "required": false },
        "active": { "type": "boolean", "required": false }
      },
      "indexes": [
        { "name": "person_name_idx", "kind": "non_unique", "fields": ["name"] },
        { "name": "person_age_idx", "kind": "ordered_range", "fields": ["age"] }
      ]
    }
  }
}
```

Semantics:

- registers a server-known object type called `person`
- synthesizes a Nexum `ObjectType`
- persists the type through the engine schema layer
- makes future `person` document operations valid

### Put Document

```json
{
  "object": {
    "action": "put",
    "type": "person",
    "key": "person-1",
    "document": {
      "id": "person-1",
      "name": "Ada",
      "age": 37,
      "active": true
    }
  }
}
```

Semantics:

- validates the document against the registered type
- converts JSON fields to Nexum `FieldValue` instances
- saves or replaces the stored object

### Get Document

```json
{
  "object": {
    "action": "get",
    "type": "person",
    "key": "person-1"
  }
}
```

### Delete Document

```json
{
  "object": {
    "action": "delete",
    "type": "person",
    "key": "person-1"
  }
}
```

### Scan Documents

```json
{
  "object": {
    "action": "scan",
    "type": "person"
  }
}
```

### Find Documents

First version should stay narrow and support exact-match selectors only:

```json
{
  "object": {
    "action": "find",
    "type": "person",
    "selector": {
      "name": { "eq": "Ada" }
    }
  }
}
```

That gives a CouchDB-like feel without overpromising query semantics that the current object facade does not yet expose.

For v1, the selector grammar should be intentionally strict:

- `selector` must be a JSON object
- `selector` must contain exactly one field name
- that field must contain exactly one operator
- the only supported v1 operator is `eq`

Valid example:

```json
{
  "object": {
    "action": "find",
    "type": "person",
    "selector": {
      "name": { "eq": "Ada" }
    }
  }
}
```

Invalid because it tries to match multiple fields in one request:

```json
{
  "object": {
    "action": "find",
    "type": "person",
    "selector": {
      "name": { "eq": "Ada" },
      "active": { "eq": true }
    }
  }
}
```

Invalid because it tries to stack operators on one field:

```json
{
  "object": {
    "action": "find",
    "type": "person",
    "selector": {
      "name": {
        "eq": "Ada",
        "prefix": "Ad"
      }
    }
  }
}
```

This is the best first shape because it is easy to validate, easy to explain, and directly aligned with the exact-match lookup capabilities the object facade already implies. If Nexum later adds richer object query semantics, the most natural extension would be multi-field `AND` matching, but that should not be part of the first server contract.

### Patch Document

If partial update support is added later, it should be modeled as a separate action rather than folded into `put`:

```json
{
  "object": {
    "action": "patch",
    "type": "person",
    "key": "person-1",
    "patch": {
      "set": {
        "name": "Ada Lovelace",
        "active": true
      },
      "inc": {
        "loginCount": 1
      }
    }
  }
}
```

That keeps full-document replacement and partial mutation clearly separated.

## Why Action-Based Object Payloads

This is intentionally closer to DynamoDB-style operations than to a freeform string query language.

Reasons:

- the current object facade is typed and explicit
- object operations are not naturally text-query shaped
- type registration must be structured
- validation and field typing are easier to define in JSON than in a custom DSL

The resulting HTTP experience still feels document-oriented and pleasant to use, especially when combined with stable keys and JSON documents.

## Object Type Registration Model

The object server layer should own a registry of exposed object types.

That registry should:

- map external type names such as `person`
- keep the registered field definitions and index definitions
- synthesize the Nexum-side `ObjectType`
- perform JSON-to-`FieldValue` conversions
- perform `FieldValue`-to-JSON conversions on reads

This avoids forcing external clients to know about Java codecs while preserving explicit schemas under the hood.

## Reference Fields

If object references are exposed, keep them explicit in JSON.

Recommended shape:

```json
{
  "addressRef": {
    "type": "address",
    "key": "addr-1"
  }
}
```

That maps cleanly to Nexum `ReferenceValue` semantics while staying readable for API clients.

## Object Action Contracts

The first implementation should define each `object` action as a strict request contract with a predictable response shape.

### Common Request Rules

Every object request payload:

- must be a JSON object
- must contain `action`
- must contain `type`
- may only contain fields defined for that action
- should fail with `400 Bad Request` if unknown top-level fields are present

Base shape:

```json
{
  "object": {
    "action": "<action-name>",
    "type": "<registered-type>"
  }
}
```

### `registerType`

Request fields:

- required: `action`, `type`, `definition`
- optional: none

`definition` fields:

- required: `keyField`, `fields`
- optional: `indexes`

Success response:

```json
{
  "results": [
    {
      "result": {
        "id": "person",
        "properties": {
          "entityType": "objectType",
          "type": "person",
          "registered": true,
          "keyField": "id"
        }
      }
    }
  ]
}
```

Failure cases:

- `type` already exists with incompatible shape
- `keyField` does not reference a defined field
- field definitions use unsupported types
- index definitions reference unknown fields

### `put`

Request fields:

- required: `action`, `type`, `key`, `document`
- optional: none

Rules:

- `key` must match the logical identity of the document
- `document` must conform to the registered schema
- if the key field is present inside `document`, it should match `key`
- `put` is a full-document write, not a partial merge

Success response:

```json
{
  "results": [
    {
      "result": {
        "id": "person-1",
        "properties": {
          "entityType": "object",
          "type": "person",
          "stored": true
        }
      }
    }
  ]
}
```

Failure cases:

- unknown object type
- missing required fields
- field value does not match the registered type
- `key` conflicts with the document identity field

### `get`

Request fields:

- required: `action`, `type`, `key`
- optional: none

Success response:

```json
{
  "results": [
    {
      "object": {
        "id": "person-1",
        "properties": {
          "entityType": "object",
          "type": "person",
          "document": {
            "id": "person-1",
            "name": "Ada",
            "age": 37
          }
        }
      }
    }
  ]
}
```

Recommended missing-document behavior:

- return `results: []` when the key is not found

### `delete`

Request fields:

- required: `action`, `type`, `key`
- optional: none

Success response:

```json
{
  "results": [
    {
      "result": {
        "id": "person-1",
        "properties": {
          "entityType": "object",
          "type": "person",
          "deleted": true
        }
      }
    }
  ]
}
```

Recommended missing-document behavior:

- deleting a missing key should still return success with `deleted: false`

### `scan`

Request fields:

- required: `action`, `type`
- optional: none in v1

Success response:

```json
{
  "results": [
    {
      "object": {
        "id": "person-1",
        "properties": {
          "entityType": "object",
          "type": "person",
          "document": {
            "id": "person-1",
            "name": "Ada"
          }
        }
      }
    },
    {
      "object": {
        "id": "person-2",
        "properties": {
          "entityType": "object",
          "type": "person",
          "document": {
            "id": "person-2",
            "name": "Grace"
          }
        }
      }
    }
  ]
}
```

Rules:

- `scan` returns all stored documents for the registered type
- ordering should be considered unspecified in v1
- pagination is not part of the first contract

### `find`

Request fields:

- required: `action`, `type`, `selector`
- optional: none in v1

Selector rules:

- `selector` must contain exactly one field
- that field must contain exactly one operator
- the only supported v1 operator is `eq`

Success response:

```json
{
  "results": [
    {
      "object": {
        "id": "person-1",
        "properties": {
          "entityType": "object",
          "type": "person",
          "document": {
            "id": "person-1",
            "name": "Ada"
          }
        }
      }
    }
  ]
}
```

Rules:

- if the target field is indexed, the provider should prefer the index path
- if the target field is not indexed, the provider may fall back to scanning
- empty matches should return `results: []`

### `patch`

`patch` should remain a planned extension rather than a first-version action.

If it is added later, request fields should be:

- required: `action`, `type`, `key`, `patch`
- optional: none

Supported patch operators should be limited to:

- `set`
- `inc`

Recommended success response:

```json
{
  "results": [
    {
      "result": {
        "id": "person-1",
        "properties": {
          "entityType": "object",
          "type": "person",
          "patched": true
        }
      }
    }
  ]
}
```

## Response Shape

The current server already uses:

```json
{
  "results": [
    {
      "row": {
        "id": "u1",
        "properties": {
          "entityType": "row",
          "name": "Ada"
        }
      }
    }
  ]
}
```

To stay compatible and intentional, object results should fit the same envelope:

```json
{
  "results": [
    {
      "object": {
        "id": "person-1",
        "properties": {
          "entityType": "object",
          "type": "person",
          "document": {
            "id": "person-1",
            "name": "Ada",
            "age": 37
          }
        }
      }
    }
  ]
}
```

For mutations:

```json
{
  "results": [
    {
      "result": {
        "id": "person-1",
        "properties": {
          "entityType": "object",
          "type": "person",
          "stored": true
        }
      }
    }
  ]
}
```

This preserves the current `results[]` convention while allowing object payloads to carry document bodies.

## Error Handling

Object operations should match the current server behavior and return `400 Bad Request` for:

- unknown provider name
- malformed payload
- unknown object action
- unknown type
- invalid field name
- invalid field type
- missing required field
- selector shapes not supported by the current implementation

That keeps object behavior compatible with current SQL/Cypher request validation.

## Operator Semantics

The operator vocabulary should stay very small and be documented explicitly.

### Selector Operators

#### `eq`

Exact-match comparison for a single field.

Example:

```json
{
  "selector": {
    "name": { "eq": "Ada" }
  }
}
```

Rules:

1. `selector` must be a JSON object
2. each key inside `selector` is a field name
3. for v1, exactly one field selector should be accepted
4. for v1, each field may specify only one operator
5. `eq` means exact field equality
6. if the field is indexed, the provider should use the exact-match index path
7. if the field is not indexed, the provider may fall back to scanning that registered type

Supported field types:

- `string`
- `long`
- `double`
- `boolean`
- `reference`

Reference values should not introduce a separate `ref` operator. Instead, reference equality should be expressed through `eq` with a typed reference value:

```json
{
  "selector": {
    "addressRef": {
      "eq": {
        "type": "address",
        "key": "addr-1"
      }
    }
  }
}
```

That keeps the operator vocabulary smaller while still supporting reference matching.

### Patch Operators

Patch operators should only be valid inside the `patch` action payload.

#### `set`

Replace one or more fields with supplied values.

Example:

```json
{
  "patch": {
    "set": {
      "name": "Ada Lovelace",
      "active": true
    }
  }
}
```

Rules:

1. `set` must be a JSON object mapping field names to values
2. each assigned value must conform to the registered field type
3. required-field constraints must still be satisfied after applying the patch
4. the key field should be immutable through `patch`
5. reference fields should use the same typed value shape used by `eq`

#### `inc`

Increment one or more numeric fields by the supplied numeric delta.

Example:

```json
{
  "patch": {
    "inc": {
      "loginCount": 1,
      "balance": 12.5
    }
  }
}
```

Rules:

1. `inc` must be a JSON object mapping field names to numeric deltas
2. `inc` is valid only for `long` and `double` fields
3. `inc` is invalid for `string`, `boolean`, and `reference` fields
4. if the field is missing and the schema does not define a valid numeric default, the request should fail
5. numeric overflow or invalid coercion should fail with `400 Bad Request`

### Unknown Operators

Unknown operators should always fail fast with `400 Bad Request`.

Invalid example:

```json
{
  "selector": {
    "name": { "prefix": "Ad" }
  }
}
```

That keeps the contract explicit and avoids accidental drift toward undocumented query semantics.

## First Version Scope

Recommended v1 scope:

- `registerType`
- `put`
- `get`
- `delete`
- `scan`
- `find` with exact-match selectors only
- `find` limited to exactly one field and one `eq` operator per request

Not recommended for v1:

- schema-less documents
- arbitrary nested selectors
- multi-field selector matching
- partial update operators like `set` and `inc`
- revision trees or CouchDB `_rev` semantics
- sort/limit/skip unless backed cleanly by the facade

## Why This Looks Intentional

This proposal keeps the best part of the current server API:

- one endpoint
- one provider key
- provider-dispatched execution

But upgrades the payload contract so providers are no longer artificially limited to a single string.

That gives Nexum:

- `sql` and `cypher` for text-query providers
- `object` for structured document operations
- one unified `/query` surface
- backward compatibility for current callers

## Summary

Recommended direction:

1. keep `POST /query`
2. require exactly one top-level provider key
3. normalize string payloads to `{ "queryText": ... }`
4. evolve providers to accept normalized JSON payloads
5. add an `object` provider with action-oriented JSON requests

This preserves the elegance of the current API while making object storage a first-class citizen of the same server contract.
