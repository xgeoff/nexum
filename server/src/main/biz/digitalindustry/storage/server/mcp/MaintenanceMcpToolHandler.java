package biz.digitalindustry.storage.server.mcp;

import biz.digitalindustry.storage.server.service.GraphStore;
import jakarta.inject.Singleton;

import java.util.List;
import java.util.Map;

@Singleton
public class MaintenanceMcpToolHandler implements McpToolHandler {
    private static final String STATUS = "nexum.maintenance.status";
    private static final String CHECKPOINT = "nexum.maintenance.checkpoint";

    private final GraphStore graphStore;

    public MaintenanceMcpToolHandler(GraphStore graphStore) {
        this.graphStore = graphStore;
    }

    @Override
    public List<McpToolDescriptor> descriptors() {
        return List.of(
                new McpToolDescriptor(STATUS, "Return the current graph WAL and checkpoint state.", Map.of("type", "object")),
                new McpToolDescriptor(CHECKPOINT, "Trigger a graph checkpoint and return the resulting maintenance status.", Map.of("type", "object"))
        );
    }

    @Override
    public boolean supports(String toolName) {
        return STATUS.equals(toolName) || CHECKPOINT.equals(toolName);
    }

    @Override
    public Object handle(String toolName, Map<String, Object> arguments) {
        if (CHECKPOINT.equals(toolName)) {
            graphStore.checkpoint();
        }
        return Map.of(
                "walSizeBytes", graphStore.walSizeBytes(),
                "checkpointRequested", graphStore.needsCheckpoint()
        );
    }
}
