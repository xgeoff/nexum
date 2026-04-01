package biz.digitalindustry.db.server.queryhandler;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.*;

@Singleton
public class QueryHandlerRegistry {
    private final Map<String, QueryHandler> handlers = new HashMap<>();

    /**
     * Construct a registry from an arbitrary set of handlers mapped by query type.
     *
     * @param handlers map of query type to handler
     */
    public QueryHandlerRegistry(Map<String, QueryHandler> handlers) {
        this.handlers.putAll(handlers);
    }

    /**
     * Convenience constructor used by DI to register the built-in handlers.
     */
    @Inject
    public QueryHandlerRegistry(CypherQueryHandler cypherHandler, SqlQueryHandler sqlHandler) {
        this(Map.of(
                "cypher", cypherHandler,
                "sql", sqlHandler
        ));
    }

    public Optional<QueryHandler> getHandler(String key) {
        return Optional.ofNullable(handlers.get(key));
    }
}
