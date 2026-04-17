package biz.digitalindustry.db.query.cypher;

import biz.digitalindustry.db.graph.api.GraphStore;
import biz.digitalindustry.db.graph.model.GraphEdgeRecord;
import biz.digitalindustry.db.graph.model.GraphNodeRecord;
import biz.digitalindustry.db.query.QueryNode;
import biz.digitalindustry.db.query.QueryCommand;
import biz.digitalindustry.db.query.QueryProvider;
import biz.digitalindustry.db.query.QueryResult;
import biz.digitalindustry.db.query.TextQuerySupport;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CypherQueryProvider implements QueryProvider {
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

    public CypherQueryProvider(GraphStore graphStore) {
        this.graphStore = graphStore;
    }

    @Override
    public String queryType() {
        return "cypher";
    }

    @Override
    public QueryResult execute(QueryCommand command) {
        String query = TextQuerySupport.requireQueryText(command);
        Matcher matcher = CREATE_NODE.matcher(query);
        if (matcher.matches()) {
            GraphNodeRecord node = graphStore.upsertNode(matcher.group(2), matcher.group(1));
            return new QueryResult(List.of(Map.of("node", toNode(node))));
        }

        matcher = CREATE_EDGE.matcher(query);
        if (matcher.matches()) {
            double weight = matcher.group(4) == null ? 1.0 : Double.parseDouble(matcher.group(4));
            GraphEdgeRecord edge = graphStore.connectNodes(
                    matcher.group(2), matcher.group(1),
                    matcher.group(6), matcher.group(5),
                    matcher.group(3), weight);
            return new QueryResult(List.of(Map.of("edge", toEdgeNode(edge))));
        }

        matcher = MATCH_BY_ID.matcher(query);
        if (matcher.matches()) {
            GraphNodeRecord node = graphStore.getNode(matcher.group(2));
            return new QueryResult(node == null ? List.of() : List.of(Map.of("node", toNode(node))));
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
            return new QueryResult(node == null ? List.of() : List.of(Map.of("node", toNode(node))));
        }

        matcher = UPDATE_EDGE_WEIGHT.matcher(query);
        if (matcher.matches()) {
            GraphEdgeRecord edge = graphStore.updateEdgeWeight(
                    matcher.group(1), matcher.group(3), matcher.group(2), Double.parseDouble(matcher.group(4)));
            return new QueryResult(edge == null ? List.of() : List.of(Map.of("edge", toEdgeNode(edge))));
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

        throw new IllegalArgumentException(
                "Unsupported cypher query. Supported forms include create, match, outgoing traversal, update, delete, and edge listing");
    }

    private QueryResult toResponse(List<GraphNodeRecord> nodes, String key) {
        List<Map<String, QueryNode>> results = new ArrayList<>(nodes.size());
        for (GraphNodeRecord node : nodes) {
            results.add(Map.of(key, toNode(node)));
        }
        return new QueryResult(results);
    }

    private QueryResult toEdgeResponse(List<GraphEdgeRecord> edges, String key) {
        List<Map<String, QueryNode>> results = new ArrayList<>(edges.size());
        for (GraphEdgeRecord edge : edges) {
            results.add(Map.of(key, toEdgeNode(edge)));
        }
        return new QueryResult(results);
    }

    private QueryResult deletionResponse(boolean deleted, String entityType) {
        if (!deleted) {
            return new QueryResult(List.of());
        }
        return new QueryResult(List.of(Map.of("result", new QueryNode("deleted", Map.of(
                "entityType", entityType,
                "deleted", true
        )))));
    }

    private QueryNode toNode(GraphNodeRecord node) {
        Map<String, Object> properties = new LinkedHashMap<>();
        properties.put("entityType", "node");
        properties.put("label", node.label());
        properties.put("outgoingCount", node.outgoingCount());
        properties.put("incomingCount", node.incomingCount());
        return new QueryNode(node.id(), properties);
    }

    private QueryNode toEdgeNode(GraphEdgeRecord edge) {
        Map<String, Object> properties = new LinkedHashMap<>();
        properties.put("entityType", "edge");
        properties.put("fromId", edge.fromId());
        properties.put("toId", edge.toId());
        properties.put("type", edge.type());
        properties.put("weight", edge.weight());
        return new QueryNode(edge.id(), properties);
    }
}
