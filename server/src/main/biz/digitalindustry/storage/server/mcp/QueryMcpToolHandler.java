package biz.digitalindustry.storage.server.mcp;

import biz.digitalindustry.storage.query.QueryCommand;
import biz.digitalindustry.storage.query.QueryNode;
import biz.digitalindustry.storage.query.QueryProvider;
import biz.digitalindustry.storage.query.QueryProviderRegistry;
import biz.digitalindustry.storage.query.QueryResult;
import jakarta.inject.Singleton;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Singleton
public class QueryMcpToolHandler implements McpToolHandler {
    private static final String TOOL_NAME = "nexum.query";

    private final QueryProviderRegistry registry;

    public QueryMcpToolHandler(QueryProviderRegistry registry) {
        this.registry = registry;
    }

    @Override
    public List<McpToolDescriptor> descriptors() {
        return List.of(new McpToolDescriptor(
                TOOL_NAME,
                "Execute a registered Nexum query provider such as sql, cypher, or object.",
                Map.of(
                        "type", "object",
                        "required", List.of("provider", "payload"),
                        "properties", Map.of(
                                "provider", Map.of("type", "string"),
                                "payload", Map.of("type", List.of("string", "object"))
                        )
                )
        ));
    }

    @Override
    public boolean supports(String toolName) {
        return TOOL_NAME.equals(toolName);
    }

    @Override
    public Object handle(String toolName, Map<String, Object> arguments) {
        String providerName = requireString(arguments, "provider");
        QueryCommand command = new QueryCommand(providerName, normalizePayload(arguments.get("payload")));
        QueryProvider provider = registry.getProvider(providerName)
                .orElseThrow(() -> new IllegalArgumentException("Unsupported query type: " + providerName));
        return Map.of(
                "provider", providerName,
                "results", toRows(provider.execute(command))
        );
    }

    private Map<String, Object> normalizePayload(Object payload) {
        if (payload instanceof String queryText) {
            if (queryText.isBlank()) {
                throw new IllegalArgumentException("queryText must not be blank");
            }
            return Map.of("queryText", queryText);
        }
        if (payload instanceof Map<?, ?> rawMap) {
            Map<String, Object> normalized = new LinkedHashMap<>();
            for (Map.Entry<?, ?> entry : rawMap.entrySet()) {
                if (!(entry.getKey() instanceof String key)) {
                    throw new IllegalArgumentException("query payload keys must be strings");
                }
                normalized.put(key, entry.getValue());
            }
            return normalized;
        }
        throw new IllegalArgumentException("payload must be either a string or an object");
    }

    private List<Map<String, Object>> toRows(QueryResult result) {
        List<Map<String, Object>> rows = new ArrayList<>();
        for (Map<String, QueryNode> row : result.rows()) {
            Map<String, Object> converted = new LinkedHashMap<>();
            for (Map.Entry<String, QueryNode> entry : row.entrySet()) {
                converted.put(entry.getKey(), Map.of(
                        "id", entry.getValue().id(),
                        "properties", entry.getValue().properties()
                ));
            }
            rows.add(converted);
        }
        return rows;
    }

    private String requireString(Map<String, Object> arguments, String field) {
        Object value = arguments.get(field);
        if (value instanceof String string && !string.isBlank()) {
            return string;
        }
        throw new IllegalArgumentException("'" + field + "' must be a non-empty string");
    }
}
