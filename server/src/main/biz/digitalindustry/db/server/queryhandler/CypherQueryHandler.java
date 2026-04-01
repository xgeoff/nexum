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

@Singleton
public class CypherQueryHandler implements QueryHandler {
    private static final Pattern CREATE_NODE = Pattern.compile(
            "^CREATE\\s*\\(\\s*\\w+\\s*:\\s*([A-Za-z][A-Za-z0-9_]*)\\s*\\{\\s*id\\s*:\\s*'([^']+)'\\s*}\\s*\\)\\s*$",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern CREATE_EDGE = Pattern.compile(
            "^CREATE\\s*\\(\\s*\\w+\\s*(?::\\s*([A-Za-z][A-Za-z0-9_]*))?\\s*\\{\\s*id\\s*:\\s*'([^']+)'\\s*}\\s*\\)\\s*-\\s*\\[:\\s*([A-Za-z][A-Za-z0-9_]*)(?:\\s*\\{\\s*weight\\s*:\\s*([0-9]+(?:\\.[0-9]+)?)\\s*})?\\s*]\\s*->\\s*\\(\\s*\\w+\\s*(?::\\s*([A-Za-z][A-Za-z0-9_]*))?\\s*\\{\\s*id\\s*:\\s*'([^']+)'\\s*}\\s*\\)\\s*$",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern MATCH_ALL = Pattern.compile(
            "^MATCH\\s*\\(\\s*(\\w+)\\s*\\)\\s*RETURN\\s+\\1\\s*$",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern MATCH_BY_ID = Pattern.compile(
            "^MATCH\\s*\\(\\s*(\\w+)\\s*\\{\\s*id\\s*:\\s*'([^']+)'\\s*}\\s*\\)\\s*RETURN\\s+\\1\\s*$",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern MATCH_BY_LABEL = Pattern.compile(
            "^MATCH\\s*\\(\\s*(\\w+)\\s*:\\s*([A-Za-z][A-Za-z0-9_]*)\\s*\\)\\s*RETURN\\s+\\1\\s*$",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern MATCH_OUTGOING_NEIGHBORS = Pattern.compile(
            "^MATCH\\s*\\(\\s*\\w+\\s*\\{\\s*id\\s*:\\s*'([^']+)'\\s*}\\s*\\)\\s*-\\s*\\[:\\s*([A-Za-z][A-Za-z0-9_]*)\\s*]\\s*->\\s*\\(\\s*(\\w+)\\s*\\)\\s*RETURN\\s+\\3\\s*$",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern MATCH_OUTGOING_NEIGHBORS_ANY = Pattern.compile(
            "^MATCH\\s*\\(\\s*\\w+\\s*\\{\\s*id\\s*:\\s*'([^']+)'\\s*}\\s*\\)\\s*-\\s*\\[\\s*]\\s*->\\s*\\(\\s*(\\w+)\\s*\\)\\s*RETURN\\s+\\2\\s*$",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern MATCH_OUTGOING_EDGES = Pattern.compile(
            "^MATCH\\s*\\(\\s*\\w+\\s*\\{\\s*id\\s*:\\s*'([^']+)'\\s*}\\s*\\)\\s*-\\s*\\[:\\s*([A-Za-z][A-Za-z0-9_]*)\\s*]\\s*->\\s*\\(\\s*\\w+\\s*\\)\\s*RETURN\\s+edge\\s*$",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern MATCH_ALL_EDGES = Pattern.compile(
            "^MATCH\\s*\\(\\s*\\w+\\s*\\)\\s*-\\s*\\[\\s*]\\s*->\\s*\\(\\s*\\w+\\s*\\)\\s*RETURN\\s+edge\\s*$",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern UPDATE_NODE_LABEL = Pattern.compile(
            "^MATCH\\s*\\(\\s*\\w+\\s*\\{\\s*id\\s*:\\s*'([^']+)'\\s*}\\s*\\)\\s*SET\\s+\\w+\\s*:\\s*([A-Za-z][A-Za-z0-9_]*)\\s*$",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern UPDATE_EDGE_WEIGHT = Pattern.compile(
            "^MATCH\\s*\\(\\s*\\w+\\s*\\{\\s*id\\s*:\\s*'([^']+)'\\s*}\\s*\\)\\s*-\\s*\\[\\s*\\w*\\s*:\\s*([A-Za-z][A-Za-z0-9_]*)\\s*]\\s*->\\s*\\(\\s*\\w+\\s*\\{\\s*id\\s*:\\s*'([^']+)'\\s*}\\s*\\)\\s*SET\\s+\\w+\\.weight\\s*=\\s*([0-9]+(?:\\.[0-9]+)?)\\s*$",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern DELETE_EDGE = Pattern.compile(
            "^MATCH\\s*\\(\\s*\\w+\\s*\\{\\s*id\\s*:\\s*'([^']+)'\\s*}\\s*\\)\\s*-\\s*\\[\\s*\\w*\\s*:\\s*([A-Za-z][A-Za-z0-9_]*)\\s*]\\s*->\\s*\\(\\s*\\w+\\s*\\{\\s*id\\s*:\\s*'([^']+)'\\s*}\\s*\\)\\s*DELETE\\s+\\w+\\s*$",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern DELETE_NODE = Pattern.compile(
            "^MATCH\\s*\\(\\s*\\w+\\s*\\{\\s*id\\s*:\\s*'([^']+)'\\s*}\\s*\\)\\s*DELETE\\s+\\w+\\s*$",
            Pattern.CASE_INSENSITIVE);

    private final GraphStore graphStore;

    public CypherQueryHandler(GraphStore graphStore) {
        this.graphStore = graphStore;
    }

    @Override
    public QueryResponse handle(String query) {
        Matcher matcher = CREATE_NODE.matcher(query);
        if (matcher.matches()) {
            GraphNodeRecord node = graphStore.upsertNode(matcher.group(2), matcher.group(1));
            return new QueryResponse(List.of(Map.of("node", toNode(node))));
        }

        matcher = CREATE_EDGE.matcher(query);
        if (matcher.matches()) {
            double weight = matcher.group(4) == null ? 1.0 : Double.parseDouble(matcher.group(4));
            GraphEdgeRecord edge = graphStore.connectNodes(
                    matcher.group(2), matcher.group(1),
                    matcher.group(6), matcher.group(5),
                    matcher.group(3), weight);
            return new QueryResponse(List.of(Map.of("edge", toEdgeNode(edge))));
        }

        matcher = MATCH_BY_ID.matcher(query);
        if (matcher.matches()) {
            GraphNodeRecord node = graphStore.getNode(matcher.group(2));
            return new QueryResponse(node == null ? List.of() : List.of(Map.of("node", toNode(node))));
        }

        matcher = MATCH_BY_LABEL.matcher(query);
        if (matcher.matches()) {
            return toResponse(graphStore.getNodesByLabel(matcher.group(2)), "node");
        }

        matcher = MATCH_OUTGOING_NEIGHBORS.matcher(query);
        if (matcher.matches()) {
            return toResponse(graphStore.getOutgoingNeighbors(matcher.group(1), matcher.group(2)), "node");
        }

        matcher = MATCH_OUTGOING_NEIGHBORS_ANY.matcher(query);
        if (matcher.matches()) {
            return toResponse(graphStore.getOutgoingNeighbors(matcher.group(1), null), "node");
        }

        matcher = MATCH_OUTGOING_EDGES.matcher(query);
        if (matcher.matches()) {
            return toEdgeResponse(graphStore.getOutgoingEdges(matcher.group(1), matcher.group(2)), "edge");
        }

        matcher = UPDATE_NODE_LABEL.matcher(query);
        if (matcher.matches()) {
            GraphNodeRecord node = graphStore.updateNodeLabel(matcher.group(1), matcher.group(2));
            return new QueryResponse(node == null ? List.of() : List.of(Map.of("node", toNode(node))));
        }

        matcher = UPDATE_EDGE_WEIGHT.matcher(query);
        if (matcher.matches()) {
            GraphEdgeRecord edge = graphStore.updateEdgeWeight(
                    matcher.group(1), matcher.group(3), matcher.group(2), Double.parseDouble(matcher.group(4)));
            return new QueryResponse(edge == null ? List.of() : List.of(Map.of("edge", toEdgeNode(edge))));
        }

        matcher = DELETE_EDGE.matcher(query);
        if (matcher.matches()) {
            boolean deleted = graphStore.deleteEdge(matcher.group(1), matcher.group(3), matcher.group(2));
            return deletionResponse(deleted, "edge");
        }

        matcher = DELETE_NODE.matcher(query);
        if (matcher.matches()) {
            boolean deleted = graphStore.deleteNode(matcher.group(1));
            return deletionResponse(deleted, "node");
        }

        if (MATCH_ALL.matcher(query).matches()) {
            return toResponse(graphStore.getAllNodes(), "node");
        }

        if (MATCH_ALL_EDGES.matcher(query).matches()) {
            return toEdgeResponse(graphStore.getAllEdges(), "edge");
        }

        throw new HttpStatusException(HttpStatus.BAD_REQUEST,
                "Unsupported cypher query. Supported forms include create, match, outgoing traversal, update, delete, and edge listing");
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

    private QueryResponse deletionResponse(boolean deleted, String entityType) {
        if (!deleted) {
            return new QueryResponse(List.of());
        }
        return new QueryResponse(List.of(Map.of("result", new Node("deleted", Map.of(
                "entityType", entityType,
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
