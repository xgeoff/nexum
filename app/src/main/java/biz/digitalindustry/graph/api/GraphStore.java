package biz.digitalindustry.graph.api;

import biz.digitalindustry.graph.model.GraphEdgeRecord;
import biz.digitalindustry.graph.model.GraphNodeRecord;

import java.util.List;

public interface GraphStore extends AutoCloseable {
    GraphNodeRecord upsertNode(String id, String label);
    GraphNodeRecord updateNodeLabel(String id, String label);
    GraphNodeRecord getNode(String id);
    List<GraphNodeRecord> getAllNodes();
    List<GraphNodeRecord> getNodesByLabel(String label);

    GraphEdgeRecord connectNodes(String fromId, String fromLabel, String toId, String toLabel, String type, double weight);
    List<GraphEdgeRecord> getAllEdges();
    List<GraphEdgeRecord> getOutgoingEdges(String nodeId, String type);
    List<GraphEdgeRecord> getIncomingEdges(String nodeId, String type);
    List<GraphNodeRecord> getOutgoingNeighbors(String nodeId, String type);
    List<GraphNodeRecord> getIncomingNeighbors(String nodeId, String type);
    GraphEdgeRecord updateEdgeWeight(String fromId, String toId, String type, double weight);
    boolean deleteEdge(String fromId, String toId, String type);
    boolean deleteNode(String id);

    @Override
    void close();
}
