package biz.digitalindustry.db.server.mcp;

import java.util.Map;

public record McpRequest(
        String jsonrpc,
        Object id,
        String method,
        Map<String, Object> params
) {
}
