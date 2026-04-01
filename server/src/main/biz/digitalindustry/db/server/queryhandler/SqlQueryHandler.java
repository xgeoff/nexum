package biz.digitalindustry.db.server.queryhandler;

import biz.digitalindustry.graph.api.GraphStore;
import biz.digitalindustry.graph.model.GraphEdgeRecord;
import biz.digitalindustry.graph.model.GraphNodeRecord;
import biz.digitalindustry.db.server.model.Node;
import biz.digitalindustry.db.server.model.QueryResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.exceptions.HttpStatusException;
import jakarta.inject.Singleton;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Minimal SQL handler backed by the graph store.
 */
@Singleton
public class SqlQueryHandler implements QueryHandler {
    private static final Pattern INSERT_NODE = Pattern.compile(
            "^INSERT\\s+INTO\\s+nodes\\s*\\(\\s*id\\s*,\\s*label\\s*\\)\\s*VALUES\\s*\\(\\s*'([^']+)'\\s*,\\s*'([^']+)'\\s*\\)\\s*$",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern INSERT_EDGE = Pattern.compile(
            "^INSERT\\s+INTO\\s+edges\\s*\\(\\s*from_id\\s*,\\s*to_id\\s*,\\s*type\\s*,\\s*weight\\s*\\)\\s*VALUES\\s*\\(\\s*'([^']+)'\\s*,\\s*'([^']+)'\\s*,\\s*'([^']+)'\\s*,\\s*([0-9]+(?:\\.[0-9]+)?)\\s*\\)\\s*$",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern SELECT_ALL = Pattern.compile(
            "^SELECT\\s+\\*\\s+FROM\\s+nodes\\s*$",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern SELECT_BY_ID = Pattern.compile(
            "^SELECT\\s+\\*\\s+FROM\\s+nodes\\s+WHERE\\s+id\\s*=\\s*'([^']+)'\\s*$",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern SELECT_BY_LABEL = Pattern.compile(
            "^SELECT\\s+\\*\\s+FROM\\s+nodes\\s+WHERE\\s+label\\s*=\\s*'([^']+)'\\s*$",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern SELECT_ALL_EDGES = Pattern.compile(
            "^SELECT\\s+\\*\\s+FROM\\s+edges\\s*$",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern SELECT_EDGES_BY_FROM = Pattern.compile(
            "^SELECT\\s+\\*\\s+FROM\\s+edges\\s+WHERE\\s+from_id\\s*=\\s*'([^']+)'\\s*$",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern SELECT_EDGES_BY_TO = Pattern.compile(
            "^SELECT\\s+\\*\\s+FROM\\s+edges\\s+WHERE\\s+to_id\\s*=\\s*'([^']+)'\\s*$",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern SELECT_EDGES_BY_TYPE = Pattern.compile(
            "^SELECT\\s+\\*\\s+FROM\\s+edges\\s+WHERE\\s+type\\s*=\\s*'([^']+)'\\s*$",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern SELECT_NEIGHBORS = Pattern.compile(
            "^SELECT\\s+\\*\\s+FROM\\s+nodes\\s+WHERE\\s+outgoing_from\\s*=\\s*'([^']+)'(?:\\s+AND\\s+edge_type\\s*=\\s*'([^']+)')?\\s*$",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern UPDATE_NODE = Pattern.compile(
            "^UPDATE\\s+nodes\\s+SET\\s+label\\s*=\\s*'([^']+)'\\s+WHERE\\s+id\\s*=\\s*'([^']+)'\\s*$",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern UPDATE_EDGE = Pattern.compile(
            "^UPDATE\\s+edges\\s+SET\\s+weight\\s*=\\s*([0-9]+(?:\\.[0-9]+)?)\\s+WHERE\\s+from_id\\s*=\\s*'([^']+)'\\s+AND\\s+to_id\\s*=\\s*'([^']+)'\\s+AND\\s+type\\s*=\\s*'([^']+)'\\s*$",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern DELETE_NODE = Pattern.compile(
            "^DELETE\\s+FROM\\s+nodes\\s+WHERE\\s+id\\s*=\\s*'([^']+)'\\s*$",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern DELETE_EDGE = Pattern.compile(
            "^DELETE\\s+FROM\\s+edges\\s+WHERE\\s+from_id\\s*=\\s*'([^']+)'\\s+AND\\s+to_id\\s*=\\s*'([^']+)'\\s+AND\\s+type\\s*=\\s*'([^']+)'\\s*$",
            Pattern.CASE_INSENSITIVE);

    private final GraphStore graphStore;

    public SqlQueryHandler(GraphStore graphStore) {
        this.graphStore = graphStore;
    }

    @Override
    public QueryResponse handle(String query) {
        Matcher matcher = INSERT_NODE.matcher(query);
        if (matcher.matches()) {
            GraphNodeRecord node = graphStore.upsertNode(matcher.group(1), matcher.group(2));
            return new QueryResponse(List.of(Map.of("row", toNode(node))));
        }

        matcher = INSERT_EDGE.matcher(query);
        if (matcher.matches()) {
            GraphEdgeRecord edge = graphStore.connectNodes(matcher.group(1), null, matcher.group(2), null,
                    matcher.group(3), Double.parseDouble(matcher.group(4)));
            return new QueryResponse(List.of(Map.of("row", toEdgeNode(edge))));
        }

        matcher = SELECT_BY_ID.matcher(query);
        if (matcher.matches()) {
            GraphNodeRecord node = graphStore.getNode(matcher.group(1));
            return new QueryResponse(node == null ? List.of() : List.of(Map.of("row", toNode(node))));
        }

        matcher = SELECT_BY_LABEL.matcher(query);
        if (matcher.matches()) {
            return toResponse(graphStore.getNodesByLabel(matcher.group(1)), "row");
        }

        matcher = UPDATE_NODE.matcher(query);
        if (matcher.matches()) {
            GraphNodeRecord node = graphStore.updateNodeLabel(matcher.group(2), matcher.group(1));
            return new QueryResponse(node == null ? List.of() : List.of(Map.of("row", toNode(node))));
        }

        matcher = UPDATE_EDGE.matcher(query);
        if (matcher.matches()) {
            GraphEdgeRecord edge = graphStore.updateEdgeWeight(
                    matcher.group(2), matcher.group(3), matcher.group(4), Double.parseDouble(matcher.group(1)));
            return new QueryResponse(edge == null ? List.of() : List.of(Map.of("row", toEdgeNode(edge))));
        }

        matcher = DELETE_NODE.matcher(query);
        if (matcher.matches()) {
            return deletionResponse(graphStore.deleteNode(matcher.group(1)));
        }

        matcher = DELETE_EDGE.matcher(query);
        if (matcher.matches()) {
            return deletionResponse(graphStore.deleteEdge(matcher.group(1), matcher.group(2), matcher.group(3)));
        }

        matcher = SELECT_NEIGHBORS.matcher(query);
        if (matcher.matches()) {
            return toResponse(graphStore.getOutgoingNeighbors(matcher.group(1), matcher.group(2)), "row");
        }

        if (SELECT_ALL.matcher(query).matches()) {
            return toResponse(graphStore.getAllNodes(), "row");
        }

        matcher = SELECT_EDGES_BY_FROM.matcher(query);
        if (matcher.matches()) {
            return toEdgeResponse(graphStore.getOutgoingEdges(matcher.group(1), null), "row");
        }

        matcher = SELECT_EDGES_BY_TO.matcher(query);
        if (matcher.matches()) {
            return toEdgeResponse(graphStore.getIncomingEdges(matcher.group(1), null), "row");
        }

        matcher = SELECT_EDGES_BY_TYPE.matcher(query);
        if (matcher.matches()) {
            List<GraphEdgeRecord> matches = new ArrayList<>();
            for (GraphEdgeRecord edge : graphStore.getAllEdges()) {
                if (matcher.group(1).equals(edge.type())) {
                    matches.add(edge);
                }
            }
            return toEdgeResponse(matches, "row");
        }

        if (SELECT_ALL_EDGES.matcher(query).matches()) {
            return toEdgeResponse(graphStore.getAllEdges(), "row");
        }

        throw new HttpStatusException(HttpStatus.BAD_REQUEST,
                "Unsupported sql query. Supported forms include INSERT/SELECT for nodes, INSERT/SELECT for edges, and outgoing neighbor lookup");
    }

    private QueryResponse toResponse(List<GraphNodeRecord> nodes, String key) {
        List<Map<String, Node>> results = new ArrayList<>(nodes.size());
        for (GraphNodeRecord node : nodes) {
            results.add(Map.of(key, toNode(node)));
        }
        return new QueryResponse(results);
    }

    private QueryResponse toEdgeResponse(List<GraphEdgeRecord> edges, String key) {
        List<Map<String, Node>> results = new ArrayList<>(edges.size());
        for (GraphEdgeRecord edge : edges) {
            results.add(Map.of(key, toEdgeNode(edge)));
        }
        return new QueryResponse(results);
    }

    private QueryResponse deletionResponse(boolean deleted) {
        if (!deleted) {
            return new QueryResponse(List.of());
        }
        return new QueryResponse(List.of(Map.of("row", new Node("deleted", Map.of(
                "entityType", "result",
                "deleted", true
        )))));
    }

    private Node toNode(GraphNodeRecord node) {
        Map<String, Object> properties = new LinkedHashMap<>();
        properties.put("entityType", "node");
        properties.put("label", node.label());
        properties.put("outgoingCount", node.outgoingCount());
        properties.put("incomingCount", node.incomingCount());
        return new Node(node.id(), properties);
    }

    private Node toEdgeNode(GraphEdgeRecord edge) {
        Map<String, Object> properties = new LinkedHashMap<>();
        properties.put("entityType", "edge");
        properties.put("fromId", edge.fromId());
        properties.put("toId", edge.toId());
        properties.put("type", edge.type());
        properties.put("weight", edge.weight());
        return new Node(edge.id(), properties);
    }
}
