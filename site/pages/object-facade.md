---
title = "Object Facade"
description = "Typed object persistence over the native record engine using explicit schemas and codecs."
layout = "reference"
eyebrow = "API Reference"
lead = "The object facade stays explicit: object types are registered intentionally, codecs control persistence shape, and the engine owns identity and indexing underneath."
statusLabel = "Persistence Style"
accent = "Explicit codecs, no magic"
---

# Object Facade

Status: active native implementation

This facade provides object-style persistence on top of the native record engine.

## Key Types

- [`ObjectStore.java`](../app/src/main/java/biz/digitalindustry/storage/object/api/ObjectStore.java)
- [`ObjectType.java`](../app/src/main/java/biz/digitalindustry/storage/object/api/ObjectType.java)
- [`ObjectCodec.java`](../app/src/main/java/biz/digitalindustry/storage/object/api/ObjectCodec.java)
- [`ObjectStoreContext.java`](../app/src/main/java/biz/digitalindustry/storage/object/api/ObjectStoreContext.java)
- [`StoredObject.java`](../app/src/main/java/biz/digitalindustry/storage/object/api/StoredObject.java)
- [`NativeObjectStore.java`](../app/src/main/java/biz/digitalindustry/storage/object/engine/NativeObjectStore.java)

## Design

The facade is explicit:

- no persistence inheritance
- no bytecode enhancement
- no runtime schema inference

Callers register `ObjectType<T>` definitions and supply an `ObjectCodec<T>` that maps domain objects to and from stored fields.

## Capabilities

Current API surface:

- register type
- save by logical key
- get by logical key
- scan all objects of a type
- exact-match lookup by indexed or non-indexed field
- delete by logical key
- typed reference resolution through `ReferenceValue`

The reserved `objectKey` field acts as the stable logical identity inside each type namespace.

## Example

```java
try (biz.digitalindustry.storage.object.api.ObjectStore store =
             new biz.digitalindustry.storage.object.engine.NativeObjectStore("data/object-store.dbs")) {
    store.registerType(addressType);
    store.registerType(personType);

    store.save(addressType, new Address("addr-1", "Oakland"));
    store.save(personType, new Person("person-1", "Ada", new Address("addr-1", "Oakland")));

    StoredObject<Person> loaded = store.get(personType, "person-1");
    System.out.println(loaded.value().address().city());
}
```

## Indexing

The object facade uses the shared engine-owned exact-match index path for:

- declared single-field indexes
- the reserved `objectKey` field

Those indexes are persisted in the native `.indexes` sidecar and reused on reopen.

## Current Limits

- no reflective POJO mapping
- no lazy proxy layer
- no query planner beyond exact-match lookup
- no higher-level relation helpers beyond explicit references
