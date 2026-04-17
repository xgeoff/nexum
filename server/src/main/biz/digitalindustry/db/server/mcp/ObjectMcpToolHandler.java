package biz.digitalindustry.db.server.mcp;

import biz.digitalindustry.db.server.service.ObjectStoreService;
import jakarta.inject.Singleton;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Singleton
public class ObjectMcpToolHandler implements McpToolHandler {
    private static final String REGISTER_TYPE = "nexum.object.register_type";
    private static final String PUT = "nexum.object.put";
    private static final String GET = "nexum.object.get";
    private static final String FIND = "nexum.object.find";
    private static final String DELETE = "nexum.object.delete";

    private final ObjectStoreService objectStore;

    public ObjectMcpToolHandler(ObjectStoreService objectStore) {
        this.objectStore = objectStore;
    }

    @Override
    public List<McpToolDescriptor> descriptors() {
        return List.of(
                new McpToolDescriptor(
                        REGISTER_TYPE,
                        "Register an object type definition in the Nexum object store.",
                        Map.of("type", "object", "required", List.of("type", "definition"))
                ),
                new McpToolDescriptor(
                        PUT,
                        "Store an object document by type and key.",
                        Map.of("type", "object", "required", List.of("type", "key", "document"))
                ),
                new McpToolDescriptor(
                        GET,
                        "Fetch an object document by type and key.",
                        Map.of("type", "object", "required", List.of("type", "key"))
                ),
                new McpToolDescriptor(
                        FIND,
                        "Find object documents by the current one-field eq selector.",
                        Map.of("type", "object", "required", List.of("type", "selector"))
                ),
                new McpToolDescriptor(
                        DELETE,
                        "Delete an object document by type and key.",
                        Map.of("type", "object", "required", List.of("type", "key"))
                )
        );
    }

    @Override
    public boolean supports(String toolName) {
        return REGISTER_TYPE.equals(toolName)
                || PUT.equals(toolName)
                || GET.equals(toolName)
                || FIND.equals(toolName)
                || DELETE.equals(toolName);
    }

    @Override
    public Object handle(String toolName, Map<String, Object> arguments) {
        return switch (toolName) {
            case REGISTER_TYPE -> {
                String type = requireString(arguments, "type");
                Map<String, Object> definition = requireObject(arguments, "definition");
                ObjectStoreService.RegisteredTypeInfo info = objectStore.registerType(type, definition);
                yield Map.of("type", info.type(), "keyField", info.keyField(), "registered", true);
            }
            case PUT -> {
                String type = requireString(arguments, "type");
                String key = requireString(arguments, "key");
                Map<String, Object> document = requireObject(arguments, "document");
                ObjectStoreService.StoredDocument stored = objectStore.put(type, key, document);
                yield stored == null ? Map.of("stored", false) : toStoredDocument(stored);
            }
            case GET -> {
                String type = requireString(arguments, "type");
                String key = requireString(arguments, "key");
                ObjectStoreService.StoredDocument stored = objectStore.get(type, key);
                yield stored == null ? Map.of("found", false) : toStoredDocument(stored);
            }
            case FIND -> {
                String type = requireString(arguments, "type");
                Map<String, Object> selector = requireObject(arguments, "selector");
                if (selector.size() != 1) {
                    throw new IllegalArgumentException("selector must contain exactly one field");
                }
                Map.Entry<String, Object> fieldEntry = selector.entrySet().iterator().next();
                Map<String, Object> operator = requireObject(fieldEntry.getValue(), "selector." + fieldEntry.getKey());
                if (operator.size() != 1 || !operator.containsKey("eq")) {
                    throw new IllegalArgumentException("selector operator must be exactly one 'eq'");
                }
                List<ObjectStoreService.StoredDocument> results = objectStore.findEq(type, fieldEntry.getKey(), operator.get("eq"));
                yield Map.of("results", results.stream().map(this::toStoredDocument).toList());
            }
            case DELETE -> {
                String type = requireString(arguments, "type");
                String key = requireString(arguments, "key");
                ObjectStoreService.DeleteResult deleted = objectStore.delete(type, key);
                yield Map.of("key", deleted.key(), "deleted", deleted.deleted());
            }
            default -> throw new IllegalArgumentException("Unsupported MCP tool: " + toolName);
        };
    }

    private Map<String, Object> toStoredDocument(ObjectStoreService.StoredDocument stored) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("key", stored.key());
        payload.put("document", stored.document());
        payload.put("found", true);
        return payload;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> requireObject(Map<String, Object> arguments, String field) {
        return requireObject(arguments.get(field), field);
    }

    private Map<String, Object> requireObject(Object value, String field) {
        if (value instanceof Map<?, ?> rawMap) {
            Map<String, Object> normalized = new LinkedHashMap<>();
            for (Map.Entry<?, ?> entry : rawMap.entrySet()) {
                if (!(entry.getKey() instanceof String key)) {
                    throw new IllegalArgumentException("'" + field + "' must use string keys");
                }
                normalized.put(key, entry.getValue());
            }
            return normalized;
        }
        throw new IllegalArgumentException("'" + field + "' must be an object");
    }

    private String requireString(Map<String, Object> arguments, String field) {
        Object value = arguments.get(field);
        if (value instanceof String string && !string.isBlank()) {
            return string;
        }
        throw new IllegalArgumentException("'" + field + "' must be a non-empty string");
    }
}
