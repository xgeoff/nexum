package biz.digitalindustry.storage.server.controller;

import biz.digitalindustry.storage.server.model.MaintenanceStatusResponse;
import biz.digitalindustry.storage.server.model.Node;
import biz.digitalindustry.storage.server.model.QueryRequest;
import biz.digitalindustry.storage.server.model.QueryResponse;
import biz.digitalindustry.storage.server.service.GraphStore;
import biz.digitalindustry.storage.server.service.ObjectStoreService;
import biz.digitalindustry.storage.server.service.RelationalStoreService;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

abstract class QueryControllerContractTest {

    @Inject
    @Client("/")
    HttpClient client;

    @Inject
    GraphStore graphStore;

    @Inject
    RelationalStoreService relationalStore;

    @Inject
    ObjectStoreService objectStore;

    @BeforeEach
    void resetStore() throws IOException {
        graphStore.reset();
        relationalStore.reset();
        objectStore.reset();
    }

    @Test
    void testPostCypherQueryPersistsAndReturnsStoredNode() {
        client.toBlocking().exchange(HttpRequest.POST("/query",
                QueryRequest.of("cypher", "CREATE (n:Person {id:'alice'})")), QueryResponse.class);

        QueryRequest request = QueryRequest.of("cypher", "MATCH (n {id:'alice'}) RETURN n");

        HttpRequest<QueryRequest> httpRequest = HttpRequest.POST("/query", request);
        HttpResponse<QueryResponse> response = client.toBlocking().exchange(httpRequest, QueryResponse.class);

        assertEquals(HttpStatus.OK, response.getStatus());
        QueryResponse body = response.body();
        assertNotNull(body);
        assertNotNull(body.getResults());
        assertFalse(body.getResults().isEmpty());

        Map<String, Node> row = body.getResults().get(0);
        assertTrue(row.containsKey("node"));

        Node node = row.get("node");
        assertEquals("alice", node.getId());
        assertEquals("node", node.getProperties().get("entityType"));
        assertEquals("Person", node.getProperties().get("label"));
        assertEquals(0, node.getProperties().get("outgoingCount"));
    }

    @Test
    void testPostSqlQueryUsesRelationalStorage() {
        client.toBlocking().exchange(HttpRequest.POST("/query",
                QueryRequest.of("sql", "CREATE TABLE users (id STRING PRIMARY KEY, name STRING NOT NULL, age LONG NOT NULL, active BOOLEAN NOT NULL)")),
                QueryResponse.class);
        client.toBlocking().exchange(HttpRequest.POST("/query",
                QueryRequest.of("sql", "INSERT INTO users (id, name, age, active) VALUES ('bob', 'Bob', 41, true)")), QueryResponse.class);

        QueryRequest request = QueryRequest.of("sql", "SELECT * FROM users WHERE id = 'bob'");

        HttpRequest<QueryRequest> httpRequest = HttpRequest.POST("/query", request);
        HttpResponse<QueryResponse> response = client.toBlocking().exchange(httpRequest, QueryResponse.class);

        assertEquals(HttpStatus.OK, response.getStatus());
        QueryResponse body = response.body();
        assertNotNull(body);
        assertFalse(body.getResults().isEmpty());

        Map<String, Node> row = body.getResults().get(0);
        Node node = row.get("row");
        assertNotNull(node);
        assertEquals("bob", node.getId());
        assertEquals("row", node.getProperties().get("entityType"));
        assertEquals("Bob", node.getProperties().get("name"));
        assertEquals(41L, ((Number) node.getProperties().get("age")).longValue());
    }

    @Test
    void testCypherCreateEdgeAndTraverseOutgoingNeighbors() {
        client.toBlocking().exchange(HttpRequest.POST("/query",
                QueryRequest.of("cypher", "CREATE (a:Person {id:'alice'})-[:KNOWS {weight:2.5}]->(b:Person {id:'bob'})")),
                QueryResponse.class);

        HttpResponse<QueryResponse> neighborResponse = client.toBlocking().exchange(
                HttpRequest.POST("/query", QueryRequest.of("cypher", "MATCH (a {id:'alice'})-[:KNOWS]->(b) RETURN b")),
                QueryResponse.class);

        QueryResponse neighborBody = neighborResponse.body();
        assertNotNull(neighborBody);
        assertEquals(1, neighborBody.getResults().size());
        Node neighbor = neighborBody.getResults().get(0).get("node");
        assertEquals("bob", neighbor.getId());
        assertEquals("Person", neighbor.getProperties().get("label"));

        HttpResponse<QueryResponse> edgeResponse = client.toBlocking().exchange(
                HttpRequest.POST("/query", QueryRequest.of("cypher", "MATCH (a)-[]->(b) RETURN edge")),
                QueryResponse.class);

        QueryResponse edgeBody = edgeResponse.body();
        assertNotNull(edgeBody);
        assertEquals(1, edgeBody.getResults().size());
        Node edge = edgeBody.getResults().get(0).get("edge");
        assertEquals("edge", edge.getProperties().get("entityType"));
        assertEquals("alice", edge.getProperties().get("fromId"));
        assertEquals("bob", edge.getProperties().get("toId"));
        assertEquals("KNOWS", edge.getProperties().get("type"));
        assertEquals(2.5, edge.getProperties().get("weight"));
    }

    @Test
    void testSqlCreateIndexAndRangeQueryRows() {
        client.toBlocking().exchange(HttpRequest.POST("/query",
                QueryRequest.of("sql", "CREATE TABLE users (id STRING PRIMARY KEY, name STRING NOT NULL, age LONG NOT NULL, active BOOLEAN NOT NULL)")),
                QueryResponse.class);
        client.toBlocking().exchange(HttpRequest.POST("/query",
                QueryRequest.of("sql", "CREATE INDEX users_name_idx ON users (name)")),
                QueryResponse.class);
        client.toBlocking().exchange(HttpRequest.POST("/query",
                QueryRequest.of("sql", "INSERT INTO users (id, name, age, active) VALUES ('u1', 'Ada', 29, true)")),
                QueryResponse.class);
        client.toBlocking().exchange(HttpRequest.POST("/query",
                QueryRequest.of("sql", "INSERT INTO users (id, name, age, active) VALUES ('u2', 'Grace', 37, false)")),
                QueryResponse.class);
        client.toBlocking().exchange(HttpRequest.POST("/query",
                QueryRequest.of("sql", "INSERT INTO users (id, name, age, active) VALUES ('u3', 'Linus', 45, true)")),
                QueryResponse.class);

        HttpResponse<QueryResponse> exactResponse = client.toBlocking().exchange(
                HttpRequest.POST("/query", QueryRequest.of("sql", "SELECT * FROM users WHERE name = 'Grace'")),
                QueryResponse.class);
        QueryResponse exactBody = exactResponse.body();
        assertNotNull(exactBody);
        assertEquals(1, exactBody.getResults().size());
        Node exact = exactBody.getResults().get(0).get("row");
        assertEquals("u2", exact.getId());
        assertEquals("Grace", exact.getProperties().get("name"));

        HttpResponse<QueryResponse> rangeResponse = client.toBlocking().exchange(
                HttpRequest.POST("/query", QueryRequest.of("sql", "SELECT * FROM users WHERE age BETWEEN 30 AND 40")),
                QueryResponse.class);
        QueryResponse rangeBody = rangeResponse.body();
        assertNotNull(rangeBody);
        assertEquals(1, rangeBody.getResults().size());
        Node ranged = rangeBody.getResults().get(0).get("row");
        assertEquals("u2", ranged.getId());
        assertEquals(37L, ((Number) ranged.getProperties().get("age")).longValue());
    }

    @Test
    void testCypherUpdateAndDeleteOperations() {
        client.toBlocking().exchange(HttpRequest.POST("/query",
                QueryRequest.of("cypher", "CREATE (a:Person {id:'alice'})-[:KNOWS {weight:1.0}]->(b:Person {id:'bob'})")),
                QueryResponse.class);

        HttpResponse<QueryResponse> updateNodeResponse = client.toBlocking().exchange(
                HttpRequest.POST("/query", QueryRequest.of("cypher", "MATCH (n {id:'alice'}) SET n:Engineer")),
                QueryResponse.class);
        Node updatedNode = updateNodeResponse.body().getResults().get(0).get("node");
        assertEquals("Engineer", updatedNode.getProperties().get("label"));

        HttpResponse<QueryResponse> updateEdgeResponse = client.toBlocking().exchange(
                HttpRequest.POST("/query", QueryRequest.of("cypher",
                        "MATCH (a {id:'alice'})-[r:KNOWS]->(b {id:'bob'}) SET r.weight = 4.5")),
                QueryResponse.class);
        Node updatedEdge = updateEdgeResponse.body().getResults().get(0).get("edge");
        assertEquals(4.5, updatedEdge.getProperties().get("weight"));

        client.toBlocking().exchange(HttpRequest.POST("/query",
                QueryRequest.of("cypher", "MATCH (a {id:'alice'})-[r:KNOWS]->(b {id:'bob'}) DELETE r")),
                QueryResponse.class);
        QueryResponse noEdges = client.toBlocking().exchange(
                HttpRequest.POST("/query", QueryRequest.of("cypher", "MATCH (a)-[]->(b) RETURN edge")),
                QueryResponse.class).body();
        assertNotNull(noEdges);
        assertTrue(noEdges.getResults().isEmpty());

        client.toBlocking().exchange(HttpRequest.POST("/query",
                QueryRequest.of("cypher", "MATCH (n {id:'bob'}) DELETE n")),
                QueryResponse.class);
        QueryResponse deletedNodeLookup = client.toBlocking().exchange(
                HttpRequest.POST("/query", QueryRequest.of("cypher", "MATCH (n {id:'bob'}) RETURN n")),
                QueryResponse.class).body();
        assertNotNull(deletedNodeLookup);
        assertTrue(deletedNodeLookup.getResults().isEmpty());
    }

    @Test
    void testSqlUpdateAndDeleteOperations() {
        client.toBlocking().exchange(HttpRequest.POST("/query",
                QueryRequest.of("sql", "CREATE TABLE users (id STRING PRIMARY KEY, name STRING NOT NULL, age LONG NOT NULL, active BOOLEAN NOT NULL)")),
                QueryResponse.class);
        client.toBlocking().exchange(HttpRequest.POST("/query",
                QueryRequest.of("sql", "INSERT INTO users (id, name, age, active) VALUES ('alice', 'Alice', 33, true)")), QueryResponse.class);
        client.toBlocking().exchange(HttpRequest.POST("/query",
                QueryRequest.of("sql", "INSERT INTO users (id, name, age, active) VALUES ('bob', 'Bob', 40, false)")),
                QueryResponse.class);

        QueryResponse updateNode = client.toBlocking().exchange(
                HttpRequest.POST("/query", QueryRequest.of("sql", "UPDATE users SET name = 'Architect', active = false WHERE id = 'alice'")),
                QueryResponse.class).body();
        assertNotNull(updateNode);
        assertEquals("Architect", updateNode.getResults().get(0).get("row").getProperties().get("name"));
        assertEquals(false, updateNode.getResults().get(0).get("row").getProperties().get("active"));

        client.toBlocking().exchange(HttpRequest.POST("/query",
                QueryRequest.of("sql", "DELETE FROM users WHERE id = 'bob'")),
                QueryResponse.class);
        QueryResponse rowsAfterDelete = client.toBlocking().exchange(
                HttpRequest.POST("/query", QueryRequest.of("sql", "SELECT * FROM users")),
                QueryResponse.class).body();
        assertNotNull(rowsAfterDelete);
        assertEquals(1, rowsAfterDelete.getResults().size());
        assertEquals("alice", rowsAfterDelete.getResults().get(0).get("row").getId());

        client.toBlocking().exchange(HttpRequest.POST("/query",
                QueryRequest.of("sql", "DELETE FROM users WHERE id = 'alice'")),
                QueryResponse.class);
        QueryResponse nodeAfterDelete = client.toBlocking().exchange(
                HttpRequest.POST("/query", QueryRequest.of("sql", "SELECT * FROM users WHERE id = 'alice'")),
                QueryResponse.class).body();
        assertNotNull(nodeAfterDelete);
        assertTrue(nodeAfterDelete.getResults().isEmpty());
    }

    @Test
    void testObjectRegisterPutGetFindPatchAndDelete() throws IOException {
        client.toBlocking().exchange(objectRequest(personTypeRegistration()),
                QueryResponse.class);

        QueryResponse putResponse = client.toBlocking().exchange(
                objectRequest(Map.of(
                        "action", "put",
                        "type", "person",
                        "key", "person-1",
                        "document", Map.of(
                                "id", "person-1",
                                "name", "Ada",
                                "age", 37,
                                "loginCount", 2,
                                "active", true
                        )
                )),
                QueryResponse.class
        ).body();
        assertNotNull(putResponse);
        assertEquals(true, putResponse.getResults().get(0).get("result").getProperties().get("stored"));

        QueryResponse getResponse = client.toBlocking().exchange(
                objectRequest(Map.of(
                        "action", "get",
                        "type", "person",
                        "key", "person-1"
                )),
                QueryResponse.class
        ).body();
        assertNotNull(getResponse);
        Node stored = getResponse.getResults().get(0).get("object");
        assertEquals("person-1", stored.getId());
        assertEquals("object", stored.getProperties().get("entityType"));
        Map<String, Object> document = castMap(stored.getProperties().get("document"));
        assertEquals("Ada", document.get("name"));
        assertEquals(37L, ((Number) document.get("age")).longValue());

        QueryResponse findResponse = client.toBlocking().exchange(
                objectRequest(Map.of(
                        "action", "find",
                        "type", "person",
                        "selector", Map.of(
                                "name", Map.of("eq", "Ada")
                        )
                )),
                QueryResponse.class
        ).body();
        assertNotNull(findResponse);
        assertEquals(1, findResponse.getResults().size());
        assertEquals("person-1", findResponse.getResults().get(0).get("object").getId());

        QueryResponse patchResponse = client.toBlocking().exchange(
                objectRequest(Map.of(
                        "action", "patch",
                        "type", "person",
                        "key", "person-1",
                        "patch", Map.of(
                                "set", Map.of(
                                        "name", "Ada Lovelace",
                                        "active", false
                                ),
                                "inc", Map.of(
                                        "loginCount", 3,
                                        "age", 1
                                )
                        )
                )),
                QueryResponse.class
        ).body();
        assertNotNull(patchResponse);
        assertEquals(true, patchResponse.getResults().get(0).get("result").getProperties().get("patched"));

        QueryResponse afterPatch = client.toBlocking().exchange(
                objectRequest(Map.of(
                        "action", "get",
                        "type", "person",
                        "key", "person-1"
                )),
                QueryResponse.class
        ).body();
        assertNotNull(afterPatch);
        Map<String, Object> patchedDocument = castMap(afterPatch.getResults().get(0).get("object").getProperties().get("document"));
        assertEquals("Ada Lovelace", patchedDocument.get("name"));
        assertEquals(false, patchedDocument.get("active"));
        assertEquals(38L, ((Number) patchedDocument.get("age")).longValue());
        assertEquals(5L, ((Number) patchedDocument.get("loginCount")).longValue());

        QueryResponse deleteResponse = client.toBlocking().exchange(
                objectRequest(Map.of(
                        "action", "delete",
                        "type", "person",
                        "key", "person-1"
                )),
                QueryResponse.class
        ).body();
        assertNotNull(deleteResponse);
        assertEquals(true, deleteResponse.getResults().get(0).get("result").getProperties().get("deleted"));

        QueryResponse afterDelete = client.toBlocking().exchange(
                objectRequest(Map.of(
                        "action", "get",
                        "type", "person",
                        "key", "person-1"
                )),
                QueryResponse.class
        ).body();
        assertNotNull(afterDelete);
        assertTrue(afterDelete.getResults().isEmpty());
    }

    @Test
    void testObjectFindRejectsMultiFieldSelector() throws IOException {
        client.toBlocking().exchange(objectRequest(personTypeRegistration()),
                QueryResponse.class);

        HttpClientResponseException ex = assertThrows(HttpClientResponseException.class,
                () -> client.toBlocking().exchange(
                        objectRequest(Map.of(
                                "action", "find",
                                "type", "person",
                                "selector", Map.of(
                                        "name", Map.of("eq", "Ada"),
                                        "active", Map.of("eq", true)
                                )
                        )),
                        QueryResponse.class
                ));

        assertEquals(HttpStatus.BAD_REQUEST, ex.getStatus());
        String body = ex.getResponse().getBody(String.class).orElse("");
        assertTrue(body.contains("exactly one field"));
    }

    @Test
    void testMissingQueryTypeReturnsError() {
        QueryRequest request = new QueryRequest();
        HttpRequest<QueryRequest> httpRequest = HttpRequest.POST("/query", request);

        HttpClientResponseException ex = assertThrows(HttpClientResponseException.class,
                () -> client.toBlocking().exchange(httpRequest, QueryResponse.class));

        assertEquals(HttpStatus.BAD_REQUEST, ex.getStatus());
        String body = ex.getResponse().getBody(String.class).orElse("");
        assertTrue(body.contains("exactly one top-level query provider") || body.contains("queryType") || body.contains("query type"));
    }

    @Test
    void testMaintenanceStatusEndpointReturnsWalState() {
        client.toBlocking().exchange(HttpRequest.POST("/query",
                QueryRequest.of("cypher", "CREATE (n:Person {id:'alice'})")), QueryResponse.class);

        HttpResponse<MaintenanceStatusResponse> response = client.toBlocking().exchange(
                HttpRequest.GET("/admin/maintenance"),
                MaintenanceStatusResponse.class
        );

        assertEquals(HttpStatus.OK, response.getStatus());
        MaintenanceStatusResponse body = response.body();
        assertNotNull(body);
        assertTrue(body.walSizeBytes() > 0L);
    }

    @Test
    void testMaintenanceCheckpointEndpointCompactsWal() {
        client.toBlocking().exchange(HttpRequest.POST("/query",
                QueryRequest.of("cypher", "CREATE (n:Person {id:'alice'})")), QueryResponse.class);

        MaintenanceStatusResponse before = client.toBlocking().exchange(
                HttpRequest.GET("/admin/maintenance"),
                MaintenanceStatusResponse.class
        ).body();
        assertNotNull(before);

        HttpResponse<MaintenanceStatusResponse> response = client.toBlocking().exchange(
                HttpRequest.POST("/admin/maintenance/checkpoint", null),
                MaintenanceStatusResponse.class
        );

        assertEquals(HttpStatus.OK, response.getStatus());
        MaintenanceStatusResponse after = response.body();
        assertNotNull(after);
        assertTrue(after.walSizeBytes() <= before.walSizeBytes());
        assertFalse(after.checkpointRequested());
    }

    private Map<String, Object> personTypeRegistration() {
        return Map.of(
                "action", "registerType",
                "type", "person",
                "definition", Map.of(
                        "keyField", "id",
                        "fields", Map.of(
                                "id", Map.of("type", "string", "required", true),
                                "name", Map.of("type", "string", "required", true),
                                "age", Map.of("type", "long", "required", true),
                                "loginCount", Map.of("type", "long", "required", true),
                                "active", Map.of("type", "boolean", "required", true)
                        ),
                        "indexes", java.util.List.of(
                                Map.of(
                                        "name", "person_name_idx",
                                        "kind", "non_unique",
                                        "fields", java.util.List.of("name")
                                )
                        )
                )
        );
    }

    private static HttpRequest<Map<String, Object>> objectRequest(Map<String, Object> payload) {
        return HttpRequest.POST("/query", Map.of("object", payload));
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> castMap(Object value) {
        return (Map<String, Object>) value;
    }
}
