package biz.digitalindustry.storage.server.mcp;

public record McpError(
        int code,
        String message
) {
}
