---
title = "Jackson Adapter"
description = "Optional Jackson-based DTO adapter for generating object definitions and saving or loading DTOs directly."
layout = "reference"
eyebrow = "Integration"
lead = "Keep Nexum core dependency-free, then add a thin Jackson layer when you want DTO-first registration and storage."
statusLabel = "Optional Module"
accent = "DTO-first convenience"
---

# Jackson Adapter

Status: optional integration module

Module: [github.com/xgeoff/nexum/tree/main/jackson](https://github.com/xgeoff/nexum/tree/main/jackson)

The Jackson adapter sits on top of the core object facade.

It gives you a DTO-first path:

- derive a Nexum `ObjectTypeDefinition` from a Jackson-visible DTO
- register that definition in the store
- save and load DTOs directly

This module is intentionally outside `lib`, so the core database stays free of Jackson dependencies.

## Key Types

- [`JacksonAdapter.java`](https://github.com/xgeoff/nexum/blob/main/jackson/src/main/java/biz/digitalindustry/db/jackson/JacksonAdapter.java)
- [`JacksonObjectTypes.java`](https://github.com/xgeoff/nexum/blob/main/jackson/src/main/java/biz/digitalindustry/db/jackson/JacksonObjectTypes.java)
- [`ObjectTypeDefinition.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/db/object/api/ObjectTypeDefinition.java)
- [`NativeObjectStore.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/db/object/runtime/NativeObjectStore.java)

## Example

```java
public record PersonDto(
        String id,
        String name,
        long age,
        boolean active
) {
}

try (var jacksonAdapter = new JacksonAdapter("./data/object.dbs")) {
    jacksonAdapter.register(PersonDto.class, "id");

    PersonDto person = new PersonDto("person-1", "Ada", 37L, true);
    jacksonAdapter.save(person);

    PersonDto loaded = jacksonAdapter.get(PersonDto.class, "person-1").value();
    System.out.println(loaded.name());
}
```

## Custom Registration

If you need indexes or explicit overrides, supply a customizer during registration:

```java
try (var jacksonAdapter = new JacksonAdapter("./data/object.dbs")) {
    jacksonAdapter.register(
            PersonDto.class,
            "id",
            builder -> builder.index("person_name_idx").on("name")
    );
}
```

The adapter uses Jackson-visible DTO properties to infer simple scalar fields:

- `String` -> `STRING`
- integral numbers -> `LONG`
- floating-point numbers -> `DOUBLE`
- `boolean` -> `BOOLEAN`

## What It Is For

Use the Jackson adapter when:

- your application already uses Jackson DTOs
- you want a small `register/save/get` API
- you want schema generation for the simple scalar cases
- you want Nexum object storage without handwritten codecs

## Current Limits

- key field still has to be specified explicitly
- references still need explicit override
- indexes still need explicit override
- nested complex object graphs are not inferred automatically
- DTO classes must be Jackson-convertible

This module is a convenience layer, not a replacement for the native object facade. When you need full control over encoding or schema shape, use the core DSL or a handwritten `ObjectCodec<T>` instead.
