package biz.digitalindustry.graph.engine;

import biz.digitalindustry.graph.api.GraphStore;
import biz.digitalindustry.graph.model.GraphEdgeRecord;
import biz.digitalindustry.graph.model.GraphNodeRecord;
import biz.digitalindustry.storage.api.StorageConfig;
import biz.digitalindustry.storage.engine.ExactMatchIndexManager;
import biz.digitalindustry.storage.engine.NativeStorageEngine;
import biz.digitalindustry.storage.model.DoubleValue;
import biz.digitalindustry.storage.model.FieldValue;
import biz.digitalindustry.storage.model.Record;
import biz.digitalindustry.storage.model.RecordId;
import biz.digitalindustry.storage.model.StringValue;
import biz.digitalindustry.storage.schema.EntityType;
import biz.digitalindustry.storage.schema.FieldDefinition;
import biz.digitalindustry.storage.schema.IndexDefinition;
import biz.digitalindustry.storage.schema.IndexKind;
import biz.digitalindustry.storage.schema.ValueType;
import biz.digitalindustry.storage.store.RecordStore;
import biz.digitalindustry.storage.tx.Transaction;
import biz.digitalindustry.storage.tx.TransactionMode;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class NativeGraphStore implements GraphStore {
    private static final int DEFAULT_PAGE_SIZE = 8192;
    private static final String NODE_NAMESPACE = "graph:nodes";
    private static final String EDGE_NAMESPACE = "graph:edges";

    private static final EntityType NODE_TYPE = new EntityType(
            "Node",
            List.of(
                    new FieldDefinition("id", ValueType.STRING, true, false),
                    new FieldDefinition("label", ValueType.STRING, false, false)
            ),
            List.of(
                    new IndexDefinition("node_id_pk", IndexKind.PRIMARY, List.of("id")),
                    new IndexDefinition("node_label_idx", IndexKind.NON_UNIQUE, List.of("label"))
            )
    );

    private static final EntityType EDGE_TYPE = new EntityType(
            "Edge",
            List.of(
                    new FieldDefinition("fromId", ValueType.STRING, true, false),
                    new FieldDefinition("toId", ValueType.STRING, true, false),
                    new FieldDefinition("type", ValueType.STRING, true, false),
                    new FieldDefinition("weight", ValueType.DOUBLE, true, false)
            ),
            List.of(
                    new IndexDefinition("edge_from_idx", IndexKind.ADJACENCY, List.of("fromId")),
                    new IndexDefinition("edge_to_idx", IndexKind.ADJACENCY, List.of("toId")),
                    new IndexDefinition("edge_type_idx", IndexKind.NON_UNIQUE, List.of("type"))
            )
    );

    private final NativeStorageEngine storageEngine;

    public NativeGraphStore(String path) {
        this.storageEngine = new NativeStorageEngine();
        this.storageEngine.open(new StorageConfig(path, DEFAULT_PAGE_SIZE));
        ensureSchema();
        rebuildIndexes();
    }

    @Override
    public GraphNodeRecord upsertNode(String id, String label) {
        try (Transaction tx = storageEngine.begin(TransactionMode.READ_WRITE)) {
            Record existing = findNodeRecord(id);
            Record stored;
            if (existing == null) {
                stored = storageEngine.recordStore().create(NODE_TYPE, new Record(
                        null,
                        NODE_TYPE,
                        nodeFields(id, label, null)
                ));
            } else {
                stored = new Record(existing.id(), NODE_TYPE, Map.of(
                        "id", new StringValue(id)
                ));
                removeNodeIndexes(existing);
                stored = new Record(existing.id(), NODE_TYPE, nodeFields(id, label, stringField(existing, "label")));
                storageEngine.recordStore().update(stored);
            }
            addNodeIndexes(stored);
            tx.commit();
            return toNode(stored);
        }
    }

    @Override
    public GraphNodeRecord updateNodeLabel(String id, String label) {
        try (Transaction tx = storageEngine.begin(TransactionMode.READ_WRITE)) {
            Record existing = findNodeRecord(id);
            if (existing == null) {
                return null;
            }
            removeNodeIndexes(existing);
            Record updated = new Record(existing.id(), NODE_TYPE, nodeFields(id, label, stringField(existing, "label")));
            storageEngine.recordStore().update(updated);
            addNodeIndexes(updated);
            tx.commit();
            return toNode(updated);
        }
    }

    @Override
    public GraphNodeRecord getNode(String id) {
        return toNode(findNodeRecord(id));
    }

    @Override
    public List<GraphNodeRecord> getAllNodes() {
        List<GraphNodeRecord> nodes = new ArrayList<>();
        for (Record record : storageEngine.recordStore().scan(NODE_TYPE)) {
            nodes.add(toNode(record));
        }
        return nodes;
    }

    @Override
    public List<GraphNodeRecord> getNodesByLabel(String label) {
        List<GraphNodeRecord> nodes = new ArrayList<>();
        for (Record record : findNodeRecords("label", new StringValue(label))) {
            if (record != null) {
                nodes.add(toNode(record));
            }
        }
        return nodes;
    }

    @Override
    public GraphEdgeRecord connectNodes(String fromId, String fromLabel, String toId, String toLabel, String type, double weight) {
        try (Transaction tx = storageEngine.begin(TransactionMode.READ_WRITE)) {
            upsertNodeInsideTx(fromId, fromLabel);
            upsertNodeInsideTx(toId, toLabel);
            Record edge = storageEngine.recordStore().create(EDGE_TYPE, new Record(
                    null,
                    EDGE_TYPE,
                    Map.of(
                            "fromId", new StringValue(fromId),
                            "toId", new StringValue(toId),
                            "type", new StringValue(type),
                            "weight", new DoubleValue(weight)
                    )
            ));
            addEdgeIndexes(edge);
            tx.commit();
            return toEdge(edge);
        }
    }

    @Override
    public List<GraphEdgeRecord> getAllEdges() {
        List<GraphEdgeRecord> edges = new ArrayList<>();
        for (Record record : storageEngine.recordStore().scan(EDGE_TYPE)) {
            edges.add(toEdge(record));
        }
        return edges;
    }

    @Override
    public List<GraphEdgeRecord> getOutgoingEdges(String nodeId, String type) {
        List<GraphEdgeRecord> edges = new ArrayList<>();
        for (Record record : findEdgeRecords("fromId", new StringValue(nodeId))) {
            if (record != null && (type == null || type.equals(stringField(record, "type")))) {
                edges.add(toEdge(record));
            }
        }
        return edges;
    }

    @Override
    public List<GraphEdgeRecord> getIncomingEdges(String nodeId, String type) {
        List<GraphEdgeRecord> edges = new ArrayList<>();
        for (Record record : findEdgeRecords("toId", new StringValue(nodeId))) {
            if (record != null && (type == null || type.equals(stringField(record, "type")))) {
                edges.add(toEdge(record));
            }
        }
        return edges;
    }

    @Override
    public List<GraphNodeRecord> getOutgoingNeighbors(String nodeId, String type) {
        List<GraphNodeRecord> nodes = new ArrayList<>();
        for (GraphEdgeRecord edge : getOutgoingEdges(nodeId, type)) {
            GraphNodeRecord node = getNode(edge.toId());
            if (node != null) {
                nodes.add(node);
            }
        }
        return nodes;
    }

    @Override
    public List<GraphNodeRecord> getIncomingNeighbors(String nodeId, String type) {
        List<GraphNodeRecord> nodes = new ArrayList<>();
        for (GraphEdgeRecord edge : getIncomingEdges(nodeId, type)) {
            GraphNodeRecord node = getNode(edge.fromId());
            if (node != null) {
                nodes.add(node);
            }
        }
        return nodes;
    }

    @Override
    public GraphEdgeRecord updateEdgeWeight(String fromId, String toId, String type, double weight) {
        try (Transaction tx = storageEngine.begin(TransactionMode.READ_WRITE)) {
            Record edge = findEdgeRecord(fromId, toId, type);
            if (edge == null) {
                return null;
            }
            removeEdgeIndexes(edge);
            Record updated = new Record(edge.id(), EDGE_TYPE, Map.of(
                    "fromId", new StringValue(fromId),
                    "toId", new StringValue(toId),
                    "type", new StringValue(type),
                    "weight", new DoubleValue(weight)
            ));
            storageEngine.recordStore().update(updated);
            addEdgeIndexes(updated);
            tx.commit();
            return toEdge(updated);
        }
    }

    @Override
    public boolean deleteEdge(String fromId, String toId, String type) {
        try (Transaction tx = storageEngine.begin(TransactionMode.READ_WRITE)) {
            Record edge = findEdgeRecord(fromId, toId, type);
            if (edge == null) {
                return false;
            }
            storageEngine.recordStore().delete(edge.id());
            removeEdgeIndexes(edge);
            tx.commit();
            return true;
        }
    }

    @Override
    public boolean deleteNode(String id) {
        try (Transaction tx = storageEngine.begin(TransactionMode.READ_WRITE)) {
            Record node = findNodeRecord(id);
            if (node == null) {
                return false;
            }
            List<RecordId> attachedEdges = new ArrayList<>();
            for (Record edge : storageEngine.recordStore().scan(EDGE_TYPE)) {
                if (id.equals(stringField(edge, "fromId")) || id.equals(stringField(edge, "toId"))) {
                    attachedEdges.add(edge.id());
                }
            }
            for (RecordId edgeId : attachedEdges) {
                Record edge = storageEngine.recordStore().get(edgeId);
                if (edge != null) {
                    storageEngine.recordStore().delete(edgeId);
                    removeEdgeIndexes(edge);
                }
            }
            storageEngine.recordStore().delete(node.id());
            removeNodeIndexes(node);
            tx.commit();
            return true;
        }
    }

    @Override
    public void close() {
        storageEngine.close();
    }

    private void ensureSchema() {
        if (storageEngine.schemaRegistry().entityType(NODE_TYPE.name()) == null) {
            storageEngine.schemaRegistry().register(NODE_TYPE);
        }
        if (storageEngine.schemaRegistry().entityType(EDGE_TYPE.name()) == null) {
            storageEngine.schemaRegistry().register(EDGE_TYPE);
        }
    }

    private void rebuildIndexes() {
        storageEngine.ensureExactMatchNamespace(
                NODE_NAMESPACE,
                List.of("id", "label"),
                NODE_TYPE,
                Record::id
        );
        storageEngine.ensureExactMatchNamespace(
                EDGE_NAMESPACE,
                List.of("fromId", "toId", "type"),
                EDGE_TYPE,
                Record::id
        );
    }

    private Record upsertNodeInsideTx(String id, String label) {
        Record existing = findNodeRecord(id);
        if (existing == null) {
            Record created = storageEngine.recordStore().create(NODE_TYPE, new Record(
                    null,
                    NODE_TYPE,
                    nodeFields(id, label, null)
            ));
            addNodeIndexes(created);
            return created;
        }
        removeNodeIndexes(existing);
        Record updated = new Record(existing.id(), NODE_TYPE, nodeFields(id, label, stringField(existing, "label")));
        storageEngine.recordStore().update(updated);
        addNodeIndexes(updated);
        return updated;
    }

    private Map<String, FieldValue> nodeFields(String id, String label, String existingLabel) {
        Map<String, FieldValue> fields = new LinkedHashMap<>();
        fields.put("id", new StringValue(id));
        String resolvedLabel = label != null ? label : existingLabel;
        if (resolvedLabel != null) {
            fields.put("label", new StringValue(resolvedLabel));
        }
        return fields;
    }

    private Record findNodeRecord(String id) {
        for (Record record : findNodeRecords("id", new StringValue(id))) {
            if (record != null) {
                return record;
            }
        }
        return null;
    }

    private Record findEdgeRecord(String fromId, String toId, String type) {
        for (Record record : findEdgeRecords("fromId", new StringValue(fromId))) {
            if (record != null
                    && toId.equals(stringField(record, "toId"))
                    && type.equals(stringField(record, "type"))) {
                return record;
            }
        }
        return null;
    }

    private List<Record> findNodeRecords(String fieldName, FieldValue value) {
        return findRecords(NODE_NAMESPACE, NODE_TYPE, fieldName, value);
    }

    private List<Record> findEdgeRecords(String fieldName, FieldValue value) {
        return findRecords(EDGE_NAMESPACE, EDGE_TYPE, fieldName, value);
    }

    private List<Record> findRecords(String namespace, EntityType type, String fieldName, FieldValue value) {
        if (indexManager().hasField(namespace, fieldName)) {
            List<Record> records = new ArrayList<>();
            for (Object id : storageEngine.exactIndexFind(namespace, fieldName, value)) {
                records.add(storageEngine.recordStore().get((RecordId) id));
            }
            return records;
        }
        List<Record> scanned = new ArrayList<>();
        for (Record record : storageEngine.recordStore().scan(type)) {
            if (value.equals(record.fields().get(fieldName))) {
                scanned.add(record);
            }
        }
        return scanned;
    }

    private void addNodeIndexes(Record record) {
        storageEngine.exactIndexAdd(NODE_NAMESPACE, List.of("id", "label"), record.fields(), record.id());
    }

    private void removeNodeIndexes(Record record) {
        storageEngine.exactIndexRemove(NODE_NAMESPACE, List.of("id", "label"), record.fields(), record.id());
    }

    private void addEdgeIndexes(Record record) {
        storageEngine.exactIndexAdd(EDGE_NAMESPACE, List.of("fromId", "toId", "type"), record.fields(), record.id());
    }

    private void removeEdgeIndexes(Record record) {
        storageEngine.exactIndexRemove(EDGE_NAMESPACE, List.of("fromId", "toId", "type"), record.fields(), record.id());
    }

    private ExactMatchIndexManager indexManager() {
        return storageEngine.indexManager();
    }

    private GraphNodeRecord toNode(Record record) {
        if (record == null) {
            return null;
        }
        String nodeId = stringField(record, "id");
        return new GraphNodeRecord(
                nodeId,
                stringField(record, "label"),
                getOutgoingEdges(nodeId, null).size(),
                getIncomingEdges(nodeId, null).size()
        );
    }

    private GraphEdgeRecord toEdge(Record record) {
        if (record == null) {
            return null;
        }
        String fromId = stringField(record, "fromId");
        String toId = stringField(record, "toId");
        String type = stringField(record, "type");
        return new GraphEdgeRecord(
                fromId + "-[" + type + "]->" + toId,
                fromId,
                toId,
                type,
                doubleField(record, "weight")
        );
    }

    private String stringField(Record record, String field) {
        FieldValue value = record.fields().get(field);
        return value instanceof StringValue stringValue ? stringValue.value() : null;
    }

    private double doubleField(Record record, String field) {
        FieldValue value = record.fields().get(field);
        return value instanceof DoubleValue doubleValue ? doubleValue.value() : 0.0d;
    }
}
