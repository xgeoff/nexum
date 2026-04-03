package biz.digitalindustry.storage.server.controller;

import biz.digitalindustry.storage.query.QueryNode;
import biz.digitalindustry.storage.query.QueryCommand;
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
        if (request.getQueries().size() != 1) {
            throw new HttpStatusException(HttpStatus.BAD_REQUEST, "request must contain exactly one top-level query provider");
        }

        String queryType = request.queryType();
        QueryCommand command = normalize(request);

        if (queryType == null || queryType.isBlank()) {
            throw new HttpStatusException(HttpStatus.BAD_REQUEST, "queryType must not be null or empty");
        }

        return registry.getProvider(queryType)
                .map(provider -> execute(provider, command))
                .orElseThrow(() -> new HttpStatusException(HttpStatus.BAD_REQUEST,
                        "Unsupported query type: " + queryType));
    }

    private QueryCommand normalize(QueryRequest request) {
        Object payload = request.query();
        if (payload instanceof String queryText) {
            if (queryText.isBlank()) {
                throw new HttpStatusException(HttpStatus.BAD_REQUEST, "queryText must not be null or empty");
            }
            return new QueryCommand(request.queryType(), Map.of("queryText", queryText));
        }
        if (payload instanceof Map<?, ?> rawMap) {
            Map<String, Object> normalized = new java.util.LinkedHashMap<>();
            for (Map.Entry<?, ?> entry : rawMap.entrySet()) {
                if (!(entry.getKey() instanceof String key)) {
                    throw new HttpStatusException(HttpStatus.BAD_REQUEST, "query payload keys must be strings");
                }
                normalized.put(key, entry.getValue());
            }
            return new QueryCommand(request.queryType(), normalized);
        }
        throw new HttpStatusException(HttpStatus.BAD_REQUEST,
                "query payload must be either a string or an object");
    }

    private QueryResponse execute(biz.digitalindustry.storage.query.QueryProvider provider, QueryCommand command) {
        try {
            return toQueryResponse(provider.execute(command));
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
