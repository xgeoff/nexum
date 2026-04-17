package biz.digitalindustry.db.server.controller;

import biz.digitalindustry.db.server.mcp.McpRequest;
import biz.digitalindustry.db.server.mcp.McpResponse;
import biz.digitalindustry.db.server.mcp.McpToolDescriptor;
import biz.digitalindustry.db.server.mcp.McpToolRegistry;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.annotation.Produces;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Controller("/mcp")
public class McpController {
    private static final int METHOD_NOT_FOUND = -32601;
    private static final int INVALID_PARAMS = -32602;
    private static final int INTERNAL_ERROR = -32603;

    private final McpToolRegistry toolRegistry;

    public McpController(McpToolRegistry toolRegistry) {
        this.toolRegistry = toolRegistry;
    }

    @Post
    @Produces(MediaType.APPLICATION_JSON)
    public McpResponse handle(@Body McpRequest request) {
        Object id = request == null ? null : request.id();
        try {
            if (request == null || request.method() == null || request.method().isBlank()) {
                return McpResponse.error(id, INVALID_PARAMS, "method must not be blank");
            }
            return switch (request.method()) {
                case "initialize" -> McpResponse.success(id, initializeResult(request.params()));
                case "tools/list" -> McpResponse.success(id, Map.of("tools", toolRegistry.tools().stream().map(this::descriptorMap).toList()));
                case "tools/call" -> McpResponse.success(id, toolCallResult(request.params()));
                default -> McpResponse.error(id, METHOD_NOT_FOUND, "Unsupported MCP method: " + request.method());
            };
        } catch (IllegalArgumentException e) {
            return McpResponse.error(id, INVALID_PARAMS, e.getMessage());
        } catch (RuntimeException e) {
            return McpResponse.error(id, INTERNAL_ERROR, e.getMessage() == null ? "internal error" : e.getMessage());
        }
    }

    private Map<String, Object> initializeResult(Map<String, Object> params) {
        String protocolVersion = "nexum-mcp-v1";
        if (params != null) {
            Object requested = params.get("protocolVersion");
            if (requested instanceof String string && !string.isBlank()) {
                protocolVersion = string;
            }
        }
        return Map.of(
                "protocolVersion", protocolVersion,
                "capabilities", Map.of(
                        "tools", Map.of("listChanged", false)
                ),
                "serverInfo", Map.of(
                        "name", "nexum-server",
                        "version", "0.1"
                )
        );
    }

    private Map<String, Object> toolCallResult(Map<String, Object> params) {
        if (params == null) {
            throw new IllegalArgumentException("tools/call requires params");
        }
        Object nameValue = params.get("name");
        if (!(nameValue instanceof String toolName) || toolName.isBlank()) {
            throw new IllegalArgumentException("tools/call requires a non-empty tool name");
        }
        Map<String, Object> arguments = new LinkedHashMap<>();
        Object rawArguments = params.get("arguments");
        if (rawArguments instanceof Map<?, ?> rawMap) {
            for (Map.Entry<?, ?> entry : rawMap.entrySet()) {
                if (!(entry.getKey() instanceof String key)) {
                    throw new IllegalArgumentException("tool arguments must use string keys");
                }
                arguments.put(key, entry.getValue());
            }
        } else if (rawArguments != null) {
            throw new IllegalArgumentException("tool arguments must be an object");
        }

        Object result = toolRegistry.call(toolName, arguments);
        return Map.of(
                "content", List.of(Map.of("type", "json", "json", result)),
                "isError", false
        );
    }

    private Map<String, Object> descriptorMap(McpToolDescriptor descriptor) {
        return Map.of(
                "name", descriptor.name(),
                "description", descriptor.description(),
                "inputSchema", descriptor.inputSchema()
        );
    }
}
