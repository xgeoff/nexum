package biz.digitalindustry.db.server.mcp;

import jakarta.inject.Singleton;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Singleton
public class McpToolRegistry {
    private final List<McpToolHandler> handlers;

    public McpToolRegistry(List<McpToolHandler> handlers) {
        this.handlers = List.copyOf(handlers);
    }

    public List<McpToolDescriptor> tools() {
        List<McpToolDescriptor> descriptors = new ArrayList<>();
        for (McpToolHandler handler : handlers) {
            descriptors.addAll(handler.descriptors());
        }
        return descriptors;
    }

    public Object call(String toolName, Map<String, Object> arguments) {
        for (McpToolHandler handler : handlers) {
            if (handler.supports(toolName)) {
                return handler.handle(toolName, arguments);
            }
        }
        throw new IllegalArgumentException("Unsupported MCP tool: " + toolName);
    }
}
