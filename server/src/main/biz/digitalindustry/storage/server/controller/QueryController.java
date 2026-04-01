package biz.digitalindustry.storage.server.controller;

import biz.digitalindustry.storage.query.QueryNode;
import biz.digitalindustry.storage.query.QueryProviderRegistry;
import biz.digitalindustry.storage.query.QueryResult;
import biz.digitalindustry.storage.server.model.QueryRequest;
import biz.digitalindustry.storage.server.model.QueryResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.*;
import io.micronaut.http.exceptions.HttpStatusException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Controller("/query")
public class QueryController {
    private final QueryProviderRegistry registry;

    public QueryController(QueryProviderRegistry registry) {
        this.registry = registry;
    }

    @Post
    @Produces(MediaType.APPLICATION_JSON)
    public QueryResponse handleQuery(@Body QueryRequest request) {
        String queryType = request.queryType();
        String query = request.query();

        if (queryType == null || queryType.isBlank()) {
            throw new HttpStatusException(HttpStatus.BAD_REQUEST, "queryType must not be null or empty");
        }
        if (query == null || query.isBlank()) {
            throw new HttpStatusException(HttpStatus.BAD_REQUEST, "query must not be null or empty");
        }

        return registry.getProvider(queryType)
                .map(provider -> execute(provider, query))
                .orElseThrow(() -> new HttpStatusException(HttpStatus.BAD_REQUEST,
                        "Unsupported query type: " + queryType));
    }

    private QueryResponse execute(biz.digitalindustry.storage.query.QueryProvider provider, String query) {
        try {
            return toQueryResponse(provider.execute(query));
        } catch (IllegalArgumentException e) {
            throw new HttpStatusException(HttpStatus.BAD_REQUEST, e.getMessage());
        }
    }

    private QueryResponse toQueryResponse(QueryResult result) {
        List<Map<String, biz.digitalindustry.storage.server.model.Node>> rows = new ArrayList<>(result.rows().size());
        for (Map<String, QueryNode> row : result.rows()) {
            Map<String, biz.digitalindustry.storage.server.model.Node> converted = new java.util.LinkedHashMap<>();
            for (Map.Entry<String, QueryNode> entry : row.entrySet()) {
                converted.put(entry.getKey(), new biz.digitalindustry.storage.server.model.Node(
                        entry.getValue().id(),
                        entry.getValue().properties()
                ));
            }
            rows.add(converted);
        }
        return new QueryResponse(rows);
    }
}
