package biz.digitalindustry.db.graph.runtime;

import biz.digitalindustry.db.graph.api.GraphStore;
import biz.digitalindustry.db.graph.model.GraphEdgeRecord;
import biz.digitalindustry.db.graph.model.GraphNodeRecord;
import biz.digitalindustry.db.engine.api.StorageConfig;
import biz.digitalindustry.db.engine.NativeStorageEngine;
import biz.digitalindustry.db.model.DoubleValue;
import biz.digitalindustry.db.model.FieldValue;
import biz.digitalindustry.db.model.Record;
import biz.digitalindustry.db.model.RecordId;
import biz.digitalindustry.db.model.StringValue;
import biz.digitalindustry.db.schema.EntityType;
import biz.digitalindustry.db.schema.FieldDefinition;
import biz.digitalindustry.db.schema.IndexDefinition;
import biz.digitalindustry.db.schema.IndexKind;
import biz.digitalindustry.db.schema.SchemaDefinition;
import biz.digitalindustry.db.schema.SchemaKind;
import biz.digitalindustry.db.schema.ValueType;
import biz.digitalindustry.db.engine.record.RecordStore;
import biz.digitalindustry.db.engine.tx.Transaction;
import biz.digitalindustry.db.engine.tx.TransactionMode;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

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
    private static final SchemaDefinition NODE_SCHEMA = SchemaDefinition.of(NODE_TYPE, SchemaKind.GRAPH);
    private static final SchemaDefinition EDGE_SCHEMA = SchemaDefinition.of(EDGE_TYPE, SchemaKind.GRAPH);

    private final NativeStorageEngine storageEngine;
    private final ReentrantLock writeLock = new ReentrantLock();

    public NativeGraphStore(String path) {
        this(StorageConfig.file(path, DEFAULT_PAGE_SIZE));
    }

    public NativeGraphStore(StorageConfig config) {
        this.storageEngine = new NativeStorageEngine();
        this.storageEngine.open(config);
        ensureSchema();
        rebuildIndexes();
    }

    public static NativeGraphStore fileBacked(String path) {
        return new NativeGraphStore(StorageConfig.file(path, DEFAULT_PAGE_SIZE));
    }

    public static NativeGraphStore memoryOnly() {
        return new NativeGraphStore(StorageConfig.memory("memory:nexum-graph", DEFAULT_PAGE_SIZE, StorageConfig.DEFAULT_MAX_WAL_BYTES));
    }

    @Override
    public GraphNodeRecord upsertNode(String id, String label) {
        return writeLocked(() -> {
            try (Transaction tx = storageEngine.begin(TransactionMode.READ_WRITE)) {
                Record existing = findNodeRecord(id);
                Record stored;
                if (existing == null) {
                    stored = storageEngine.recordStore().create(NODE_TYPE, new Record(
                            null,
                            NODE_TYPE,
                            nodeFields(id, label, null)
                    ));
                    addNodeIndexes(stored);
                } else {
                    String existingLabel = stringField(existing, "label");
                    String resolvedLabel = resolveNodeLabel(label, existingLabel);
                    if (labelsEqual(existingLabel, resolvedLabel)) {
                        tx.commit();
                        return toNode(existing);
                    }
                    removeNodeIndexes(existing);
                    stored = new Record(existing.id(), NODE_TYPE, nodeFields(id, resolvedLabel, null));
                    storageEngine.recordStore().update(stored);
                    addNodeIndexes(stored);
                }
                tx.commit();
                return toNode(stored);
            }
        });
    }

    @Override
    public GraphNodeRecord updateNodeLabel(String id, String label) {
        return writeLocked(() -> {
            try (Transaction tx = storageEngine.begin(TransactionMode.READ_WRITE)) {
                Record existing = findNodeRecord(id);
                if (existing == null) {
                    return null;
                }
                String existingLabel = stringField(existing, "label");
                String resolvedLabel = resolveNodeLabel(label, existingLabel);
                if (labelsEqual(existingLabel, resolvedLabel)) {
                    tx.commit();
                    return toNode(existing);
                }
                removeNodeIndexes(existing);
                Record updated = new Record(existing.id(), NODE_TYPE, nodeFields(id, resolvedLabel, null));
                storageEngine.recordStore().update(updated);
                addNodeIndexes(updated);
                tx.commit();
                return toNode(updated);
            }
        });
    }

    @Override
    public GraphNodeRecord getNode(String id) {
        return readLocked(() -> toNode(findNodeRecord(id)));
    }

    @Override
    public List<GraphNodeRecord> getAllNodes() {
        return readLocked(() -> {
            Map<String, NodeCounts> countsByNodeId = buildNodeCounts();
            List<GraphNodeRecord> nodes = new ArrayList<>();
            for (Record record : storageEngine.recordStore().scan(NODE_TYPE)) {
                nodes.add(toNode(record, countsByNodeId));
            }
            return nodes;
        });
    }

    @Override
    public List<GraphNodeRecord> getNodesByLabel(String label) {
        return readLocked(() -> {
            Map<String, NodeCounts> countsByNodeId = buildNodeCounts();
            List<GraphNodeRecord> nodes = new ArrayList<>();
            for (Record record : findNodeRecords("label", new StringValue(label))) {
                if (record != null) {
                    nodes.add(toNode(record, countsByNodeId));
                }
            }
            return nodes;
        });
    }

    @Override
    public GraphEdgeRecord connectNodes(String fromId, String fromLabel, String toId, String toLabel, String type, double weight) {
        return writeLocked(() -> {
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
        });
    }

    @Override
    public List<GraphEdgeRecord> getAllEdges() {
        return readLocked(() -> {
            List<GraphEdgeRecord> edges = new ArrayList<>();
            for (Record record : storageEngine.recordStore().scan(EDGE_TYPE)) {
                edges.add(toEdge(record));
            }
            return edges;
        });
    }

    @Override
    public List<GraphEdgeRecord> getOutgoingEdges(String nodeId, String type) {
        return readLocked(() -> {
            List<GraphEdgeRecord> edges = new ArrayList<>();
            for (Record record : findEdgeRecords("fromId", new StringValue(nodeId))) {
                if (record != null && (type == null || type.equals(stringField(record, "type")))) {
                    edges.add(toEdge(record));
                }
            }
            return edges;
        });
    }

    @Override
    public List<GraphEdgeRecord> getIncomingEdges(String nodeId, String type) {
        return readLocked(() -> {
            List<GraphEdgeRecord> edges = new ArrayList<>();
            for (Record record : findEdgeRecords("toId", new StringValue(nodeId))) {
                if (record != null && (type == null || type.equals(stringField(record, "type")))) {
                    edges.add(toEdge(record));
                }
            }
            return edges;
        });
    }

    @Override
    public List<GraphNodeRecord> getOutgoingNeighbors(String nodeId, String type) {
        return readLocked(() -> {
            List<GraphNodeRecord> nodes = new ArrayList<>();
            for (GraphEdgeRecord edge : findOutgoingEdgesInternal(nodeId, type)) {
                GraphNodeRecord node = toNode(findNodeRecord(edge.toId()));
                if (node != null) {
                    nodes.add(node);
                }
            }
            return nodes;
        });
    }

    @Override
    public List<GraphNodeRecord> getIncomingNeighbors(String nodeId, String type) {
        return readLocked(() -> {
            List<GraphNodeRecord> nodes = new ArrayList<>();
            for (GraphEdgeRecord edge : findIncomingEdgesInternal(nodeId, type)) {
                GraphNodeRecord node = toNode(findNodeRecord(edge.fromId()));
                if (node != null) {
                    nodes.add(node);
                }
            }
            return nodes;
        });
    }

    @Override
    public GraphEdgeRecord updateEdgeWeight(String fromId, String toId, String type, double weight) {
        return writeLocked(() -> {
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
        });
    }

    @Override
    public boolean deleteEdge(String fromId, String toId, String type) {
        return writeLocked(() -> {
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
        });
    }

    @Override
    public boolean deleteNode(String id) {
        return writeLocked(() -> {
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
        });
    }

    @Override
    public void close() {
        writeLocked(() -> {
            storageEngine.close();
            return null;
        });
    }

    public long walSizeBytes() {
        return readLocked(storageEngine::walSizeBytes);
    }

    public boolean needsCheckpoint() {
        return readLocked(storageEngine::needsCheckpoint);
    }

    public void checkpoint() {
        writeLocked(() -> {
            storageEngine.checkpoint();
            return null;
        });
    }

    private <T> T readLocked(Supplier<T> action) {
        return action.get();
    }

    private <T> T writeLocked(Supplier<T> action) {
        writeLock.lock();
        try {
            return action.get();
        } finally {
            writeLock.unlock();
        }
    }

    private List<GraphEdgeRecord> findOutgoingEdgesInternal(String nodeId, String type) {
        List<GraphEdgeRecord> edges = new ArrayList<>();
        for (Record record : findEdgeRecords("fromId", new StringValue(nodeId))) {
            if (record != null && (type == null || type.equals(stringField(record, "type")))) {
                edges.add(toEdge(record));
            }
        }
        return edges;
    }

    private List<GraphEdgeRecord> findIncomingEdgesInternal(String nodeId, String type) {
        List<GraphEdgeRecord> edges = new ArrayList<>();
        for (Record record : findEdgeRecords("toId", new StringValue(nodeId))) {
            if (record != null && (type == null || type.equals(stringField(record, "type")))) {
                edges.add(toEdge(record));
            }
        }
        return edges;
    }

    private void ensureSchema() {
        if (storageEngine.schemaRegistry().schema(NODE_TYPE.name()) == null) {
            storageEngine.schemaRegistry().register(NODE_SCHEMA);
        }
        if (storageEngine.schemaRegistry().schema(EDGE_TYPE.name()) == null) {
            storageEngine.schemaRegistry().register(EDGE_SCHEMA);
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
        String existingLabel = stringField(existing, "label");
        String resolvedLabel = resolveNodeLabel(label, existingLabel);
        if (labelsEqual(existingLabel, resolvedLabel)) {
            return existing;
        }
        removeNodeIndexes(existing);
        Record updated = new Record(existing.id(), NODE_TYPE, nodeFields(id, resolvedLabel, null));
        storageEngine.recordStore().update(updated);
        addNodeIndexes(updated);
        return updated;
    }

    private Map<String, FieldValue> nodeFields(String id, String label, String existingLabel) {
        Map<String, FieldValue> fields = new LinkedHashMap<>();
        fields.put("id", new StringValue(id));
        String resolvedLabel = resolveNodeLabel(label, existingLabel);
        if (resolvedLabel != null) {
            fields.put("label", new StringValue(resolvedLabel));
        }
        return fields;
    }

    private String resolveNodeLabel(String label, String existingLabel) {
        return label != null ? label : existingLabel;
    }

    private boolean labelsEqual(String left, String right) {
        return left == null ? right == null : left.equals(right);
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
        if (NODE_NAMESPACE.equals(namespace) ? List.of("id", "label").contains(fieldName) : List.of("fromId", "toId", "type").contains(fieldName)) {
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

    private GraphNodeRecord toNode(Record record, Map<String, NodeCounts> countsByNodeId) {
        if (record == null) {
            return null;
        }
        String nodeId = stringField(record, "id");
        NodeCounts counts = countsByNodeId.getOrDefault(nodeId, NodeCounts.ZERO);
        return new GraphNodeRecord(
                nodeId,
                stringField(record, "label"),
                counts.outgoingCount(),
                counts.incomingCount()
        );
    }

    private Map<String, NodeCounts> buildNodeCounts() {
        Map<String, NodeCounts> countsByNodeId = new LinkedHashMap<>();
        for (Record edgeRecord : storageEngine.recordStore().scan(EDGE_TYPE)) {
            String fromId = stringField(edgeRecord, "fromId");
            String toId = stringField(edgeRecord, "toId");
            if (fromId != null) {
                countsByNodeId.compute(fromId, (ignored, counts) -> (counts == null ? NodeCounts.ZERO : counts).incrementOutgoing());
            }
            if (toId != null) {
                countsByNodeId.compute(toId, (ignored, counts) -> (counts == null ? NodeCounts.ZERO : counts).incrementIncoming());
            }
        }
        return countsByNodeId;
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

    private record NodeCounts(int outgoingCount, int incomingCount) {
        private static final NodeCounts ZERO = new NodeCounts(0, 0);

        private NodeCounts incrementOutgoing() {
            return new NodeCounts(outgoingCount + 1, incomingCount);
        }

        private NodeCounts incrementIncoming() {
            return new NodeCounts(outgoingCount, incomingCount + 1);
        }
    }
}
