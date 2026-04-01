package biz.digitalindustry.db.server.controller;

import biz.digitalindustry.db.server.model.Node;
import biz.digitalindustry.db.server.model.QueryRequest;
import biz.digitalindustry.db.server.model.QueryResponse;
import biz.digitalindustry.db.server.service.GraphStore;
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

    @BeforeEach
    void resetStore() throws IOException {
        graphStore.reset();
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
    void testPostSqlQueryUsesGraphStorage() {
        client.toBlocking().exchange(HttpRequest.POST("/query",
                QueryRequest.of("sql", "INSERT INTO nodes (id, label) VALUES ('bob', 'Engineer')")), QueryResponse.class);

        QueryRequest request = QueryRequest.of("sql", "SELECT * FROM nodes WHERE id = 'bob'");

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
        assertEquals("node", node.getProperties().get("entityType"));
        assertEquals("Engineer", node.getProperties().get("label"));
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
    void testSqlInsertEdgeAndQueryEdgesAndNeighbors() {
        client.toBlocking().exchange(HttpRequest.POST("/query",
                QueryRequest.of("sql", "INSERT INTO nodes (id, label) VALUES ('alice', 'Person')")), QueryResponse.class);
        client.toBlocking().exchange(HttpRequest.POST("/query",
                QueryRequest.of("sql", "INSERT INTO nodes (id, label) VALUES ('bob', 'Person')")), QueryResponse.class);
        client.toBlocking().exchange(HttpRequest.POST("/query",
                QueryRequest.of("sql", "INSERT INTO edges (from_id, to_id, type, weight) VALUES ('alice', 'bob', 'KNOWS', 1.75)")),
                QueryResponse.class);

        HttpResponse<QueryResponse> edgeResponse = client.toBlocking().exchange(
                HttpRequest.POST("/query", QueryRequest.of("sql", "SELECT * FROM edges WHERE from_id = 'alice'")),
                QueryResponse.class);
        QueryResponse edgeBody = edgeResponse.body();
        assertNotNull(edgeBody);
        assertEquals(1, edgeBody.getResults().size());
        Node edge = edgeBody.getResults().get(0).get("row");
        assertEquals("edge", edge.getProperties().get("entityType"));
        assertEquals("KNOWS", edge.getProperties().get("type"));
        assertEquals(1.75, edge.getProperties().get("weight"));

        HttpResponse<QueryResponse> neighborResponse = client.toBlocking().exchange(
                HttpRequest.POST("/query", QueryRequest.of("sql", "SELECT * FROM nodes WHERE outgoing_from = 'alice' AND edge_type = 'KNOWS'")),
                QueryResponse.class);
        QueryResponse neighborBody = neighborResponse.body();
        assertNotNull(neighborBody);
        assertEquals(1, neighborBody.getResults().size());
        Node neighbor = neighborBody.getResults().get(0).get("row");
        assertEquals("bob", neighbor.getId());
        assertEquals("Person", neighbor.getProperties().get("label"));
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
                QueryRequest.of("sql", "INSERT INTO nodes (id, label) VALUES ('alice', 'Person')")), QueryResponse.class);
        client.toBlocking().exchange(HttpRequest.POST("/query",
                QueryRequest.of("sql", "INSERT INTO nodes (id, label) VALUES ('bob', 'Person')")), QueryResponse.class);
        client.toBlocking().exchange(HttpRequest.POST("/query",
                QueryRequest.of("sql", "INSERT INTO edges (from_id, to_id, type, weight) VALUES ('alice', 'bob', 'KNOWS', 1.0)")),
                QueryResponse.class);

        QueryResponse updateNode = client.toBlocking().exchange(
                HttpRequest.POST("/query", QueryRequest.of("sql", "UPDATE nodes SET label = 'Architect' WHERE id = 'alice'")),
                QueryResponse.class).body();
        assertNotNull(updateNode);
        assertEquals("Architect", updateNode.getResults().get(0).get("row").getProperties().get("label"));

        QueryResponse updateEdge = client.toBlocking().exchange(
                HttpRequest.POST("/query", QueryRequest.of("sql",
                        "UPDATE edges SET weight = 9.25 WHERE from_id = 'alice' AND to_id = 'bob' AND type = 'KNOWS'")),
                QueryResponse.class).body();
        assertNotNull(updateEdge);
        assertEquals(9.25, updateEdge.getResults().get(0).get("row").getProperties().get("weight"));

        client.toBlocking().exchange(HttpRequest.POST("/query",
                QueryRequest.of("sql", "DELETE FROM edges WHERE from_id = 'alice' AND to_id = 'bob' AND type = 'KNOWS'")),
                QueryResponse.class);
        QueryResponse edgesAfterDelete = client.toBlocking().exchange(
                HttpRequest.POST("/query", QueryRequest.of("sql", "SELECT * FROM edges")),
                QueryResponse.class).body();
        assertNotNull(edgesAfterDelete);
        assertTrue(edgesAfterDelete.getResults().isEmpty());

        client.toBlocking().exchange(HttpRequest.POST("/query",
                QueryRequest.of("sql", "DELETE FROM nodes WHERE id = 'alice'")),
                QueryResponse.class);
        QueryResponse nodeAfterDelete = client.toBlocking().exchange(
                HttpRequest.POST("/query", QueryRequest.of("sql", "SELECT * FROM nodes WHERE id = 'alice'")),
                QueryResponse.class).body();
        assertNotNull(nodeAfterDelete);
        assertTrue(nodeAfterDelete.getResults().isEmpty());
    }

    @Test
    void testMissingQueryTypeReturnsError() {
        QueryRequest request = new QueryRequest();
        HttpRequest<QueryRequest> httpRequest = HttpRequest.POST("/query", request);

        HttpClientResponseException ex = assertThrows(HttpClientResponseException.class,
                () -> client.toBlocking().exchange(httpRequest, QueryResponse.class));

        assertEquals(HttpStatus.BAD_REQUEST, ex.getStatus());
        String body = ex.getResponse().getBody(String.class).orElse("");
        assertTrue(body.contains("queryType") || body.contains("query type"));
    }
}
