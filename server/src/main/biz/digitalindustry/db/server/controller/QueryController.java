package biz.digitalindustry.db.server.controller;

import biz.digitalindustry.db.server.model.QueryRequest;
import biz.digitalindustry.db.server.model.QueryResponse;
import biz.digitalindustry.db.server.queryhandler.QueryHandlerRegistry;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.*;
import io.micronaut.http.exceptions.HttpStatusException;

@Controller("/query")
public class QueryController {
    private final QueryHandlerRegistry registry;

    public QueryController(QueryHandlerRegistry registry) {
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

        return registry.getHandler(queryType)
                .map(handler -> handler.handle(query))
                .orElseThrow(() -> new HttpStatusException(HttpStatus.BAD_REQUEST,
                        "Unsupported query type: " + queryType));
    }
}
