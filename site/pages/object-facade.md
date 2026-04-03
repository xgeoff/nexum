---
title = "Object Facade"
description = "Typed object persistence over the native record engine with a simple schema DSL and optional handwritten codecs."
layout = "reference"
eyebrow = "API Reference"
lead = "Start with the schema DSL and generated default codec for the simple path, then drop down to a handwritten codec only when you need explicit encoding control."
statusLabel = "Persistence Style"
accent = "DSL first, explicit control when needed"
---

# Object Facade

Status: active native implementation

This facade provides object-style persistence on top of the native record engine.

Source package: [github.com/xgeoff/nexum/tree/main/lib/src/main/java/biz/digitalindustry/storage/object](https://github.com/xgeoff/nexum/tree/main/lib/src/main/java/biz/digitalindustry/storage/object)

## Key Types

- [`ObjectStore.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/storage/object/api/ObjectStore.java)
- [`ObjectType.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/storage/object/api/ObjectType.java)
- [`ObjectCodec.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/storage/object/api/ObjectCodec.java)
- [`ObjectTypeDefinition.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/storage/object/api/ObjectTypeDefinition.java)
- [`ObjectTypes.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/storage/object/api/ObjectTypes.java)
- [`GeneratedObjectTypes.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/storage/object/api/GeneratedObjectTypes.java)
- [`ObjectStoreContext.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/storage/object/api/ObjectStoreContext.java)
- [`StoredObject.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/storage/object/api/StoredObject.java)
- [`NativeObjectStore.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/storage/object/engine/NativeObjectStore.java)

## Design

The facade is explicit:

- no persistence inheritance
- no bytecode enhancement
- no runtime schema inference

Callers can use the object facade in two ways:

- define an `ObjectTypeDefinition` with the DSL and use the generated default codec for `Map<String, Object>` documents
- define `ObjectType<T>` and supply a handwritten `ObjectCodec<T>` when you need full control

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

Start with the DSL and generated default codec unless you need custom encoding or rich domain-object mapping.

```java
ObjectTypeDefinition personDefinition = ObjectTypes.define("Person")
        .key("id")
        .string("id").required()
        .string("name").required()
        .longNumber("age").required()
        .index("person_name_idx").on("name")
        .build();

try (var store = NativeObjectStore.fileBacked("./data/object-store.dbs")) {
    ObjectType<Map<String, Object>> personType = store.registerGeneratedType(personDefinition);

    store.save(personType, Map.of(
            "id", "person-1",
            "name", "Ada",
            "age", 37L
    ));
}
```

Generated object type definitions are persisted in the engine as reserved metadata entities, so the store can rehydrate them on reopen.

That generated path should feel close to the server-side JSON experience. It is the default path for embedded callers when `Map<String, Object>` is enough.

Reference fields use the same builder style:

```java
ObjectTypeDefinition personDefinition = ObjectTypes.define("Person")
        .key("id")
        .string("id").required()
        .string("name").required()
        .reference("address").to("Address").optional()
        .build();
```

## Handwritten Codec

Use a handwritten `ObjectCodec<T>` when you want explicit control over how a domain object is encoded and decoded.

The type definitions should usually live with the domain types they describe:

```java
record Address(String id, String city) {
    static final ObjectType<Address> TYPE = new ObjectType<>(
            "Address",
            List.of(
                    new FieldDefinition("city", ValueType.STRING, true, false)
            ),
            List.of(),
            new ObjectCodec<>() {
                @Override
                public String key(Address address) {
                    return address.id();
                }

                @Override
                public Map<String, FieldValue> encode(Address address, ObjectStoreContext context) {
                    return Map.of("city", new StringValue(address.city()));
                }

                @Override
                public Address decode(StoredObjectView view, ObjectStoreContext context) {
                    return new Address(
                            view.key(),
                            ((StringValue) view.fields().get("city")).value()
                    );
                }
            }
    );
}

record Person(String id, String name, Address address) {
    static final ObjectType<Person> TYPE = new ObjectType<>(
            "Person",
            List.of(
                    new FieldDefinition("name", ValueType.STRING, true, false),
                    new FieldDefinition("address", ValueType.REFERENCE, true, false)
            ),
            List.of(),
            new ObjectCodec<>() {
                @Override
                public String key(Person person) {
                    return person.id();
                }

                @Override
                public Map<String, FieldValue> encode(Person person, ObjectStoreContext context) {
                    return Map.of(
                            "name", new StringValue(person.name()),
                            "address", context.reference(Address.TYPE, person.address().id())
                    );
                }

                @Override
                public Person decode(StoredObjectView view, ObjectStoreContext context) {
                    return new Person(
                            view.key(),
                            ((StringValue) view.fields().get("name")).value(),
                            context.resolve(Address.TYPE, (ReferenceValue) view.fields().get("address"))
                    );
                }
            }
    );
}

try (var store = NativeObjectStore.fileBacked("./data/object-store.dbs")) {
    store.registerType(Address.TYPE);
    store.registerType(Person.TYPE);

    store.save(Address.TYPE, new Address("addr-1", "Oakland"));
    store.save(Person.TYPE, new Person("person-1", "Ada", new Address("addr-1", "Oakland")));

    StoredObject<Person> loaded = store.get(Person.TYPE, "person-1");
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
