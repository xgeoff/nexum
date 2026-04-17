package biz.digitalindustry.db.query.vector;

import biz.digitalindustry.db.model.BooleanValue;
import biz.digitalindustry.db.model.DoubleValue;
import biz.digitalindustry.db.model.FieldValue;
import biz.digitalindustry.db.model.LongValue;
import biz.digitalindustry.db.model.StringValue;
import biz.digitalindustry.db.model.VectorValue;
import biz.digitalindustry.db.model.Vector;
import biz.digitalindustry.db.query.QueryCommand;
import biz.digitalindustry.db.query.QueryNode;
import biz.digitalindustry.db.query.QueryProvider;
import biz.digitalindustry.db.query.QueryResult;
import biz.digitalindustry.db.query.TextQuerySupport;
import biz.digitalindustry.db.vector.api.VectorCollectionDefinition;
import biz.digitalindustry.db.vector.api.VectorDocument;
import biz.digitalindustry.db.vector.api.VectorDocumentMatch;
import biz.digitalindustry.db.vector.api.VectorStore;
import biz.digitalindustry.db.vector.Distances;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Locale;

public final class VectorQueryProvider implements QueryProvider {
    private static final Set<String> ROOT_KEYS = Set.of("from", "vector");
    private static final Set<String> VECTOR_KEYS = Set.of("field", "nearest");
    private static final Set<String> NEAREST_KEYS = Set.of("vector", "k", "distance");
    private final VectorStore vectorStore;

    public VectorQueryProvider(VectorStore vectorStore) {
        this.vectorStore = vectorStore;
    }

    @Override
    public String queryType() {
        return "vector";
    }

    @Override
    public QueryResult execute(QueryCommand command) {
        ParsedVectorQuery query = parse(command);
        VectorCollectionDefinition collection = vectorStore.collection(query.from());
        if (collection == null) {
            throw new IllegalArgumentException("Unknown vector collection '" + query.from() + "'");
        }
        requireVectorField(collection, query.field());
        String configuredDistance = Distances.normalize(collection.distanceMetric());
        String requestedDistance = Distances.normalize(query.distance());
        if (!configuredDistance.equals(requestedDistance)) {
            throw new IllegalArgumentException(
                    "Vector query distance '" + requestedDistance + "' does not match index distance '" + configuredDistance + "'");
        }
        return rowsResult(vectorStore.nearest(collection, query.vector(), query.limit()));
    }

    private ParsedVectorQuery parse(QueryCommand command) {
        if (command.payload().containsKey("queryText")) {
            return parseText(TextQuerySupport.requireQueryText(command));
        }
        return parseJson(command.payload());
    }

    private ParsedVectorQuery parseText(String queryText) {
        String from = null;
        String field = null;
        Vector vector = null;
        Integer limit = null;
        String distance = null;
        List<String> lines = normalizedLines(queryText);
        if (lines.isEmpty()) {
            throw new IllegalArgumentException("vector queryText must not be empty");
        }
        for (String line : lines) {
            String upper = line.toUpperCase(Locale.ROOT);
            if (upper.startsWith("VECTOR FROM ")) {
                from = requireUniqueClause(from, "FROM");
                from = line.substring("VECTOR FROM ".length()).trim();
                if (from.isEmpty()) {
                    throw new IllegalArgumentException("VECTOR FROM must specify a source name");
                }
                continue;
            }
            if (upper.startsWith("FIELD ")) {
                field = requireUniqueClause(field, "FIELD");
                field = line.substring("FIELD ".length()).trim();
                if (field.isEmpty()) {
                    throw new IllegalArgumentException("FIELD must specify a field name");
                }
                continue;
            }
            if (upper.startsWith("NEAREST ")) {
                if (vector != null) {
                    throw new IllegalArgumentException("NEAREST clause may only appear once");
                }
                vector = parseVectorLiteral(line.substring("NEAREST ".length()).trim(), "NEAREST");
                continue;
            }
            if (upper.startsWith("K ")) {
                if (limit != null) {
                    throw new IllegalArgumentException("K clause may only appear once");
                }
                limit = intValue(line.substring(2).trim(), "K");
                if (limit <= 0) {
                    throw new IllegalArgumentException("K must be positive");
                }
                continue;
            }
            if (upper.startsWith("DISTANCE ")) {
                distance = requireUniqueClause(distance, "DISTANCE");
                distance = stringValue(line.substring("DISTANCE ".length()).trim(), "DISTANCE");
                continue;
            }
            throw new IllegalArgumentException("Unsupported vector query line: " + line);
        }
        return new ParsedVectorQuery(
                requireString(from, "FROM"),
                requireString(field, "FIELD"),
                requireVector(vector, "NEAREST"),
                requireInt(limit, "K"),
                requireString(distance, "DISTANCE")
        );
    }

    private ParsedVectorQuery parseJson(Map<String, Object> payload) {
        rejectUnknownKeys(payload, ROOT_KEYS, "vector");
        String from = stringValue(payload.get("from"), "from");
        Map<String, Object> vectorObject = objectValue(payload.get("vector"), "vector");
        rejectUnknownKeys(vectorObject, VECTOR_KEYS, "vector.vector");
        String field = stringValue(vectorObject.get("field"), "vector.field");
        Map<String, Object> nearestObject = objectValue(vectorObject.get("nearest"), "vector.nearest");
        rejectUnknownKeys(nearestObject, NEAREST_KEYS, "vector.nearest");
        Vector vector = vectorValue(nearestObject.get("vector"), "vector.nearest.vector");
        int limit = intValue(nearestObject.get("k"), "vector.nearest.k");
        if (limit <= 0) {
            throw new IllegalArgumentException("'vector.nearest.k' must be positive");
        }
        String distance = stringValue(nearestObject.get("distance"), "vector.nearest.distance");
        return new ParsedVectorQuery(from, field, vector, limit, distance);
    }

    private QueryResult rowsResult(List<VectorDocumentMatch> matches) {
        List<Map<String, QueryNode>> rows = new ArrayList<>();
        for (VectorDocumentMatch match : matches) {
            VectorDocument document = match.document();
            Map<String, Object> props = new LinkedHashMap<>();
            props.put("entityType", "vectorDocument");
            props.put("key", document.key());
            props.put("distance", match.distance());
            for (Map.Entry<String, FieldValue> entry : document.values().entrySet()) {
                props.put(entry.getKey(), toObject(entry.getValue()));
            }
            rows.add(Map.of("result", new QueryNode(document.key(), props)));
        }
        return new QueryResult(rows);
    }

    private Object toObject(FieldValue value) {
        if (value instanceof StringValue stringValue) {
            return stringValue.value();
        }
        if (value instanceof LongValue longValue) {
            return longValue.value();
        }
        if (value instanceof DoubleValue doubleValue) {
            return doubleValue.value();
        }
        if (value instanceof BooleanValue booleanValue) {
            return booleanValue.value();
        }
        if (value instanceof VectorValue vectorValue) {
            float[] values = vectorValue.vector().values();
            List<Float> components = new ArrayList<>(values.length);
            for (float component : values) {
                components.add(component);
            }
            return components;
        }
        return value == null ? null : value.toString();
    }

    private void requireVectorField(VectorCollectionDefinition collection, String field) {
        if (!collection.vectorField().equals(field)) {
            throw new IllegalArgumentException(
                    "Field '" + field + "' is not the configured vector field for collection '" + collection.name() + "'");
        }
    }

    private String stringValue(Object value, String field) {
        if (value instanceof String string && !string.isBlank()) {
            return string;
        }
        throw new IllegalArgumentException("'" + field + "' must be a non-empty string");
    }

    private int intValue(Object value, String field) {
        if (value instanceof Number number) {
            return number.intValue();
        }
        if (value instanceof String string && !string.isBlank()) {
            try {
                return Integer.parseInt(string);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("'" + field + "' must be numeric", e);
            }
        }
        throw new IllegalArgumentException("'" + field + "' must be numeric");
    }

    private Vector vectorValue(Object value, String field) {
        if (value instanceof Vector vector) {
            return vector;
        }
        if (value instanceof List<?> list) {
            float[] values = new float[list.size()];
            for (int i = 0; i < list.size(); i++) {
                Object item = list.get(i);
                if (!(item instanceof Number number)) {
                    throw new IllegalArgumentException("'" + field + "' must contain only numeric elements");
                }
                values[i] = number.floatValue();
            }
            return new Vector(values);
        }
        throw new IllegalArgumentException("'" + field + "' must be a vector or numeric array");
    }

    private Map<String, Object> objectValue(Object value, String field) {
        if (!(value instanceof Map<?, ?> raw)) {
            throw new IllegalArgumentException("'" + field + "' must be an object");
        }
        Map<String, Object> normalized = new LinkedHashMap<>();
        for (Map.Entry<?, ?> entry : raw.entrySet()) {
            if (!(entry.getKey() instanceof String key)) {
                throw new IllegalArgumentException("'" + field + "' must use string keys");
            }
            normalized.put(key, entry.getValue());
        }
        return normalized;
    }

    private void rejectUnknownKeys(Map<String, ?> payload, Set<String> allowed, String field) {
        for (String key : payload.keySet()) {
            if (!allowed.contains(key)) {
                throw new IllegalArgumentException("Unknown field '" + key + "' in " + field);
            }
        }
    }

    private List<String> normalizedLines(String queryText) {
        List<String> lines = new ArrayList<>();
        for (String line : queryText.split("\\R")) {
            String trimmed = line.trim();
            if (!trimmed.isEmpty()) {
                lines.add(trimmed);
            }
        }
        return lines;
    }

    private Vector parseVectorLiteral(String literal, String field) {
        String trimmed = literal.trim();
        if (!trimmed.startsWith("[") || !trimmed.endsWith("]")) {
            throw new IllegalArgumentException("'" + field + "' must be a bracketed vector literal");
        }
        String body = trimmed.substring(1, trimmed.length() - 1).trim();
        if (body.isEmpty()) {
            throw new IllegalArgumentException("'" + field + "' must not be empty");
        }
        String[] parts = body.split(",");
        float[] values = new float[parts.length];
        for (int i = 0; i < parts.length; i++) {
            String part = parts[i].trim();
            if (part.isEmpty()) {
                throw new IllegalArgumentException("'" + field + "' contains an empty vector component");
            }
            try {
                values[i] = Float.parseFloat(part);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("'" + field + "' must contain only numeric elements", e);
            }
        }
        return new Vector(values);
    }

    private String requireUniqueClause(String current, String clause) {
        if (current != null) {
            throw new IllegalArgumentException(clause + " clause may only appear once");
        }
        return current;
    }

    private String requireString(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " is required");
        }
        return value;
    }

    private Vector requireVector(Vector value, String field) {
        if (value == null) {
            throw new IllegalArgumentException(field + " is required");
        }
        return value;
    }

    private int requireInt(Integer value, String field) {
        if (value == null) {
            throw new IllegalArgumentException(field + " is required");
        }
        return value;
    }

    private record ParsedVectorQuery(
            String from,
            String field,
            Vector vector,
            int limit,
            String distance
    ) {
    }
}
