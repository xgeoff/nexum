package biz.digitalindustry.db.server.mcp;

public record McpError(
        int code,
        String message
) {
}
