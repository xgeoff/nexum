package biz.digitalindustry.db.server.queryhandler;

import biz.digitalindustry.db.server.model.QueryResponse;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class QueryHandlerRegistryTest {

    @Test
    void supportsMultipleHandlers() {
        QueryHandler cypher = q -> new QueryResponse();
        QueryHandler sql = q -> new QueryResponse();

        QueryHandlerRegistry registry = new QueryHandlerRegistry(Map.of(
                "cypher", cypher,
                "sql", sql
        ));

        assertTrue(registry.getHandler("cypher").isPresent());
        assertTrue(registry.getHandler("sql").isPresent());
        assertTrue(registry.getHandler("gremlin").isEmpty());
    }
}
