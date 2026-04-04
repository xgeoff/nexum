---
title = "Getting Started"
description = "How to build Nexum, use the core library directly, run the optional server, embed it in Spring Boot, and call the sample HTTP API."
layout = "systems"
eyebrow = "Quick Start"
lead = "Start with the core library when you want embedded storage, add the optional server when you want HTTP access, and keep both paths explicit and small."
metricLabel = "Recommended Path"
metricValue = "Embed first, serve second"
metricCopy = "The core library is the product. The server is an optional transport layer over the same storage and query surfaces."
---

# Getting Started

Repository: [github.com/xgeoff/nexum](https://github.com/xgeoff/nexum)

This guide covers:

1. building the Nexum artifacts
2. using the core library directly
3. using all three data-access modes: graph, relational, and object
4. running the provided Micronaut server
5. embedding Nexum inside another server such as Spring Boot
6. calling the sample HTTP API

## Build The Artifacts

Build the core library jar:

```bash
./gradlew :lib:jar
```

Build the optional Jackson adapter jar:

```bash
./gradlew :jackson:jar
```

Build the optional server jar:

```bash
./gradlew :server:jar
```

Resulting artifacts:

- core library: `lib/build/libs/nexum.jar`
- Jackson adapter: `jackson/build/libs/nexum-jackson.jar`
- server: `server/build/libs/nexum-server.jar`

Run the full test suite:

```bash
./gradlew test
```

Run the benchmark harness:

```bash
./gradlew :lib:benchmark
```

## Use The Core Library Directly

If you are working inside this repository, depend on the module directly:

```gradle
dependencies {
    implementation(project(":lib"))
}
```

If you want to consume the built jar from another Gradle project, copy `nexum.jar` into that project and wire it explicitly:

```gradle
dependencies {
    implementation(files("libs/nexum.jar"))
}
```

For Maven, the same local-jar approach looks like:

```xml
<dependency>
  <groupId>local.nexum</groupId>
  <artifactId>nexum</artifactId>
  <version>0</version>
  <scope>system</scope>
  <systemPath>\${project.basedir}/libs/nexum.jar</systemPath>
</dependency>
```

That `systemPath` pattern is only appropriate for local evaluation. If you plan to reuse Nexum across projects, publish the jar into your internal Maven repository instead of hard-wiring local file paths.

## Important Multi-Model Note

Nexum uses one storage engine underneath all three facades, but cross-facade retrieval is not supported by default.

- graph data is stored and read through the graph facade
- relational data is stored and read through the relational facade
- object data is stored and read through the object facade

This is intentional. Each facade defines its own schema shape, reserved identity fields, indexes, and API contract above the same physical storage engine. The engine is shared, but the higher-level models are not automatically translated into one another.

If you need that behavior, you can write a custom bridge or projection layer on top of Nexum's shared record and schema primitives. Nexum does not provide that mapping automatically because the correct translation rules are application-specific.

## Graph Mode

Use the graph facade when you want nodes, edges, and traversal directly from the embedded library.

```java
import biz.digitalindustry.storage.graph.api.GraphStore;
import biz.digitalindustry.storage.graph.engine.NativeGraphStore;

try (GraphStore graph = NativeGraphStore.fileBacked("./data/graph.dbs")) {
    graph.upsertNode("alice", "Person");
    graph.upsertNode("bob", "Person");
    graph.connectNodes("alice", "Person", "bob", "Person", "KNOWS", 1.0d);

    var neighbors = graph.getOutgoingNeighbors("alice", "KNOWS");
    System.out.println(neighbors.size());
}
```

Memory-only mode:

```java
try (GraphStore graph = NativeGraphStore.memoryOnly()) {
    graph.upsertNode("alice", "Person");
}
```

Key references:

- [`GraphStore.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/storage/graph/api/GraphStore.java)
- [`NativeGraphStore.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/storage/graph/engine/NativeGraphStore.java)

## Relational Mode

Use the relational facade when you want explicit tables, rows, and exact or range lookup.

```java
import biz.digitalindustry.storage.model.LongValue;
import biz.digitalindustry.storage.model.StringValue;
import biz.digitalindustry.storage.relational.api.Row;
import biz.digitalindustry.storage.relational.api.TableDefinition;
import biz.digitalindustry.storage.relational.engine.NativeRelationalStore;

import java.util.Map;

try (var store = NativeRelationalStore.fileBacked("./data/relational.dbs")) {
    store.registerTable(usersTable);

    store.upsert(usersTable, new Row("u1", Map.of(
            "id", new StringValue("u1"),
            "name", new StringValue("Ada"),
            "age", new LongValue(37)
    )));

    Row row = store.get(usersTable, "u1");
    System.out.println(row.fields().get("name"));
}
```

Key references:

- [`RelationalStore.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/storage/relational/api/RelationalStore.java)
- [`TableDefinition.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/storage/relational/api/TableDefinition.java)
- [`NativeRelationalStore.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/storage/relational/engine/NativeRelationalStore.java)

## Object Mode

Use the object facade when you want typed persistence. Start with the DSL and generated default codec for `Map<String, Object>` documents, use JavaBeans when you want getter/setter-based object mapping, use the optional Jackson adapter when your app is already DTO-first, and drop down to a handwritten codec only when you need custom encoding logic.

```java
import biz.digitalindustry.storage.object.api.ObjectTypeDefinition;
import biz.digitalindustry.storage.object.api.ObjectTypes;
import biz.digitalindustry.storage.object.engine.NativeObjectStore;

import java.util.Map;

ObjectTypeDefinition personDefinition = ObjectTypes.define("GeneratedPerson")
        .key("id")
        .string("id").required()
        .string("name").required()
        .longNumber("age").required()
        .index("generated_person_name_idx").on("name")
        .build();

try (var store = NativeObjectStore.fileBacked("./data/object.dbs")) {
    var personType = store.registerGeneratedType(personDefinition);
    store.save(personType, Map.of(
            "id", "person-1",
            "name", "Ada",
            "age", 37L
    ));

    var loaded = store.get(personType, "person-1");
    System.out.println(loaded.value().get("name"));
}
```

If you need explicit control over how a domain object is encoded and decoded, Nexum also supports a handwritten `ObjectCodec<T>`. That path is covered in the object facade reference.

If you prefer Java objects over `Map<String, Object>`, but still want to avoid a handwritten codec, Nexum also supports a JavaBean path:

```java
import biz.digitalindustry.storage.object.api.ObjectTypeDefinition;
import biz.digitalindustry.storage.object.api.ObjectTypes;
import biz.digitalindustry.storage.object.engine.NativeObjectStore;

public class PersonBean {
    private String id;
    private String name;
    private long age;

    public PersonBean() {
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getAge() {
        return age;
    }

    public void setAge(long age) {
        this.age = age;
    }
}

ObjectTypeDefinition personDefinition = ObjectTypes.define(PersonBean.class)
        .key("id")
        .build();

try (var store = NativeObjectStore.fileBacked("./data/object.dbs")) {
    var personType = store.registerBeanType(PersonBean.class, personDefinition);

    PersonBean person = new PersonBean();
    person.setId("person-1");
    person.setName("Ada");
    person.setAge(37L);

    store.save(personType, person);
}
```

This bean path infers the simple scalar fields from the bean itself, so you only need to specify the key and any explicit overrides such as indexes or references. If you want broader POJO support and your app already uses Jackson, use the optional [`nexum-jackson`](jackson-adapter.md) adapter instead.

If you have already registered an object definition and want to save a plain Java object without a separate codec, you can map against the stored definition directly:

```java
import biz.digitalindustry.storage.object.api.ObjectTypeDefinition;
import biz.digitalindustry.storage.object.api.ObjectTypes;
import biz.digitalindustry.storage.object.engine.NativeObjectStore;

public class PersonPojo {
    public String id;
    public String name;
    public long age;

    public PersonPojo() {
    }
}

ObjectTypeDefinition personDefinition = ObjectTypes.define("Person")
        .key("id")
        .string("id").required()
        .string("name").required()
        .longNumber("age").required()
        .build();

try (var store = NativeObjectStore.fileBacked("./data/object.dbs")) {
    store.registerGeneratedType(personDefinition);

    ObjectTypeDefinition storedDefinition = store.generatedTypeDefinition("Person");

    PersonPojo person = new PersonPojo();
    person.id = "person-1";
    person.name = "Ada";
    person.age = 37L;

    store.save(storedDefinition, person);

    var loaded = store.get(storedDefinition, PersonPojo.class, "person-1");
    System.out.println(loaded.value().name);
}
```

This mapped-object path supports JavaBeans and non-final public fields. Private-field POJOs are intentionally not auto-mapped.

For broader POJO mapping, records, and Jackson-friendly DTOs, see [`jackson-adapter.md`](jackson-adapter.md).

If your application already uses Jackson DTOs, the optional `jackson` module gives you a thinner adapter:

```java
import biz.digitalindustry.storage.jackson.JacksonAdapter;

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

That adapter lives outside the core library so Nexum itself does not take on a Jackson dependency.

Key references:

- [`ObjectStore.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/storage/object/api/ObjectStore.java)
- [`ObjectCodec.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/storage/object/api/ObjectCodec.java)
- [`ObjectTypeDefinition.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/storage/object/api/ObjectTypeDefinition.java)
- [`ObjectTypes.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/storage/object/api/ObjectTypes.java)
- [`GeneratedObjectTypes.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/storage/object/api/GeneratedObjectTypes.java)
- [`NativeObjectStore.java`](https://github.com/xgeoff/nexum/blob/main/lib/src/main/java/biz/digitalindustry/storage/object/engine/NativeObjectStore.java)
- [`JacksonAdapter.java`](https://github.com/xgeoff/nexum/blob/main/jackson/src/main/java/biz/digitalindustry/storage/jackson/JacksonAdapter.java)

## Use The Query Providers Directly

If you want query strings without the HTTP server, embed the query providers over the graph or relational facades:

```java
import biz.digitalindustry.storage.graph.engine.NativeGraphStore;
import biz.digitalindustry.storage.query.cypher.CypherQueryProvider;
import biz.digitalindustry.storage.query.sql.SqlQueryProvider;
import biz.digitalindustry.storage.relational.engine.NativeRelationalStore;

var graph = NativeGraphStore.fileBacked("./data/query-graph.dbs");
var relational = NativeRelationalStore.fileBacked("./data/query-relational.dbs");

var cypher = new CypherQueryProvider(graph);
var sql = new SqlQueryProvider(relational);
```

This is the lightest way to expose query strings inside your own application without bringing in the Nexum server module.

## Run The Provided Server

Start the server against file-backed stores:

```bash
./gradlew :server:run \
  -Dgraph.db.path=./data/nexum-graph.dbs \
  -Drelational.db.path=./data/nexum-relational.dbs
```

Run it in memory-only mode:

```bash
./gradlew :server:run \
  -Dgraph.db.mode=memory \
  -Drelational.db.mode=memory
```

Useful properties:

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

## Embed Nexum In Spring Boot

If you already have a Spring Boot application, the usual pattern is:

1. add `nexum.jar` to your application dependencies
2. create Nexum stores as Spring beans
3. inject those stores into your own controllers or services
4. optionally wrap them with your own REST API instead of using the Micronaut server module

Example configuration:

```java
package com.example.demo;

import biz.digitalindustry.storage.graph.api.GraphStore;
import biz.digitalindustry.storage.graph.engine.NativeGraphStore;
import biz.digitalindustry.storage.relational.api.RelationalStore;
import biz.digitalindustry.storage.relational.engine.NativeRelationalStore;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
class NexumConfig {

    @Bean(destroyMethod = "close")
    GraphStore graphStore() {
        return NativeGraphStore.fileBacked("./data/spring-graph.dbs");
    }

    @Bean(destroyMethod = "close")
    RelationalStore relationalStore() {
        return NativeRelationalStore.fileBacked("./data/spring-relational.dbs");
    }
}
```

Example controller:

```java
package com.example.demo;

import biz.digitalindustry.storage.graph.api.GraphStore;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
class GraphController {
    private final GraphStore graphStore;

    GraphController(GraphStore graphStore) {
        this.graphStore = graphStore;
    }

    @GetMapping("/graph/node-count")
    int nodeCount() {
        return graphStore.getAllNodes().size();
    }
}
```

That approach keeps Nexum embedded inside your preferred HTTP framework while avoiding an extra server layer.

## Call The Sample Server API

Create a graph node:

```bash
curl -X POST http://localhost:8080/query \
  -H 'Content-Type: application/json' \
  -d '{ "cypher": "CREATE (n:Person {id:'\''alice'\''})" }'
```

Read graph nodes:

```bash
curl -X POST http://localhost:8080/query \
  -H 'Content-Type: application/json' \
  -d '{ "cypher": "MATCH (n) RETURN n" }'
```

Create a relational table:

```bash
curl -X POST http://localhost:8080/query \
  -H 'Content-Type: application/json' \
  -d '{ "sql": "CREATE TABLE users (id STRING PRIMARY KEY, name STRING NOT NULL, age LONG NOT NULL, active BOOLEAN NOT NULL)" }'
```

Insert a relational row:

```bash
curl -X POST http://localhost:8080/query \
  -H 'Content-Type: application/json' \
  -d '{ "sql": "INSERT INTO users (id, name, age, active) VALUES ('\''u1'\'', '\''Ada'\'', 37, true)" }'
```

Read relational rows:

```bash
curl -X POST http://localhost:8080/query \
  -H 'Content-Type: application/json' \
  -d '{ "sql": "SELECT * FROM users WHERE id = '\''u1'\''" }'
```

Inspect maintenance status:

```bash
curl http://localhost:8080/admin/maintenance
```

Trigger a checkpoint:

```bash
curl -X POST http://localhost:8080/admin/maintenance/checkpoint
```

## Choosing A Path

Choose the core library directly when:

- you are embedding Nexum into another JVM application
- you want the smallest runtime surface
- you want direct control over lifecycle and API design

Choose the provided server when:

- you need a ready-made HTTP transport quickly
- the built-in query subset is enough for your use case
- you want operational endpoints for maintenance out of the box

Choose embedding inside another server such as Spring Boot when:

- you already have an application platform and API layer
- you want Nexum storage without committing to the Micronaut server artifact
- you need your own authentication, routing, or domain-specific API surface
