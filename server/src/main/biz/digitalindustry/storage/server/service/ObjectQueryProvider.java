package biz.digitalindustry.storage.server.service;

import biz.digitalindustry.storage.query.QueryCommand;
import biz.digitalindustry.storage.query.QueryNode;
import biz.digitalindustry.storage.query.QueryProvider;
import biz.digitalindustry.storage.query.QueryResult;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ObjectQueryProvider implements QueryProvider {
    private final ObjectStoreService objectStore;

    public ObjectQueryProvider(ObjectStoreService objectStore) {
        this.objectStore = objectStore;
    }

    @Override
    public String queryType() {
        return "object";
    }

    @Override
    public QueryResult execute(QueryCommand command) {
        Map<String, Object> payload = command.payload();
        String action = requireString(payload, "action");
        String type = requireString(payload, "type");

        return switch (action) {
            case "registerType" -> registerType(type, payload);
            case "put" -> put(type, payload);
            case "get" -> get(type, payload);
            case "delete" -> delete(type, payload);
            case "scan" -> scan(type, payload);
            case "find" -> find(type, payload);
            case "patch" -> patch(type, payload);
            default -> throw new IllegalArgumentException("Unsupported object action '" + action + "'");
        };
    }

    private QueryResult registerType(String type, Map<String, Object> payload) {
        rejectUnknownKeys(payload, Set.of("action", "type", "definition"), "object");
        Map<String, Object> definition = requireObject(payload, "definition");
        ObjectStoreService.RegisteredTypeInfo info = objectStore.registerType(type, definition);
        return singleResult("result", new QueryNode(info.type(), Map.of(
                "entityType", "objectType",
                "type", info.type(),
                "registered", true,
                "keyField", info.keyField()
        )));
    }

    private QueryResult put(String type, Map<String, Object> payload) {
        rejectUnknownKeys(payload, Set.of("action", "type", "key", "document"), "object");
        String key = requireString(payload, "key");
        Map<String, Object> document = requireObject(payload, "document");
        objectStore.put(type, key, document);
        return mutationResult(key, type, "stored", true);
    }

    private QueryResult get(String type, Map<String, Object> payload) {
        rejectUnknownKeys(payload, Set.of("action", "type", "key"), "object");
        String key = requireString(payload, "key");
        ObjectStoreService.StoredDocument document = objectStore.get(type, key);
        if (document == null) {
            return new QueryResult(List.of());
        }
        return objectRows(type, List.of(document));
    }

    private QueryResult delete(String type, Map<String, Object> payload) {
        rejectUnknownKeys(payload, Set.of("action", "type", "key"), "object");
        String key = requireString(payload, "key");
        ObjectStoreService.DeleteResult result = objectStore.delete(type, key);
        return mutationResult(result.key(), type, "deleted", result.deleted());
    }

    private QueryResult scan(String type, Map<String, Object> payload) {
        rejectUnknownKeys(payload, Set.of("action", "type"), "object");
        return objectRows(type, objectStore.scan(type));
    }

    private QueryResult find(String type, Map<String, Object> payload) {
        rejectUnknownKeys(payload, Set.of("action", "type", "selector"), "object");
        Map<String, Object> selector = requireObject(payload, "selector");
        if (selector.size() != 1) {
            throw new IllegalArgumentException("selector must contain exactly one field");
        }

        Map.Entry<String, Object> fieldEntry = selector.entrySet().iterator().next();
        Map<String, Object> operator = requireObject(fieldEntry.getValue(), "selector." + fieldEntry.getKey());
        if (operator.size() != 1 || !operator.containsKey("eq")) {
            throw new IllegalArgumentException("selector fields must contain exactly one eq operator");
        }

        return objectRows(type, objectStore.findEq(type, fieldEntry.getKey(), operator.get("eq")));
    }

    private QueryResult patch(String type, Map<String, Object> payload) {
        rejectUnknownKeys(payload, Set.of("action", "type", "key", "patch"), "object");
        String key = requireString(payload, "key");
        Map<String, Object> patch = requireObject(payload, "patch");
        ObjectStoreService.StoredDocument updated = objectStore.patch(type, key, patch);
        if (updated == null) {
            return new QueryResult(List.of());
        }
        return mutationResult(key, type, "patched", true);
    }

    private QueryResult objectRows(String type, List<ObjectStoreService.StoredDocument> documents) {
        List<Map<String, QueryNode>> rows = new ArrayList<>();
        for (ObjectStoreService.StoredDocument document : documents) {
            Map<String, Object> properties = new LinkedHashMap<>();
            properties.put("entityType", "object");
            properties.put("type", type);
            properties.put("document", document.document());
            rows.add(Map.of("object", new QueryNode(document.key(), properties)));
        }
        return new QueryResult(rows);
    }

    private QueryResult mutationResult(String key, String type, String flag, boolean value) {
        Map<String, Object> properties = new LinkedHashMap<>();
        properties.put("entityType", "object");
        properties.put("type", type);
        properties.put(flag, value);
        return singleResult("result", new QueryNode(key, properties));
    }

    private QueryResult singleResult(String key, QueryNode node) {
        return new QueryResult(List.of(Map.of(key, node)));
    }

    private static String requireString(Map<String, Object> map, String key) {
        Object value = map.get(key);
        if (value instanceof String string && !string.isBlank()) {
            return string;
        }
        throw new IllegalArgumentException("'" + key + "' must be a non-empty string");
    }

    private static Map<String, Object> requireObject(Map<String, Object> map, String key) {
        return requireObject(map.get(key), key);
    }

    private static Map<String, Object> requireObject(Object value, String key) {
        if (!(value instanceof Map<?, ?> raw)) {
            throw new IllegalArgumentException("'" + key + "' must be an object");
        }
        Map<String, Object> normalized = new LinkedHashMap<>();
        for (Map.Entry<?, ?> entry : raw.entrySet()) {
            if (!(entry.getKey() instanceof String field)) {
                throw new IllegalArgumentException("'" + key + "' must use string keys");
            }
            normalized.put(field, entry.getValue());
        }
        return normalized;
    }

    private static void rejectUnknownKeys(Map<String, ?> map, Set<String> allowed, String scope) {
        for (String key : map.keySet()) {
            if (!allowed.contains(key)) {
                throw new IllegalArgumentException("Unknown field '" + key + "' in " + scope);
            }
        }
    }
}
