package biz.digitalindustry.storage.server.mcp;

import java.util.List;
import java.util.Map;

public interface McpToolHandler {
    List<McpToolDescriptor> descriptors();

    boolean supports(String toolName);

    Object handle(String toolName, Map<String, Object> arguments);
}
