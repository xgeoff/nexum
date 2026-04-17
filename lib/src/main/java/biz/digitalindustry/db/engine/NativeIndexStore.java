package biz.digitalindustry.db.engine;

import biz.digitalindustry.db.model.BooleanValue;
import biz.digitalindustry.db.model.DoubleValue;
import biz.digitalindustry.db.model.FieldValue;
import biz.digitalindustry.db.model.LongValue;
import biz.digitalindustry.db.model.RecordId;
import biz.digitalindustry.db.model.ReferenceValue;
import biz.digitalindustry.db.model.StringValue;
import biz.digitalindustry.db.index.OrderedRangeIndex;
import biz.digitalindustry.db.engine.page.PageFile;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

final class NativeIndexStore implements AutoCloseable {
    private static final int VERSION = 10;
    private static final byte ENTRY_EXACT = 1;
    private static final byte ENTRY_ORDERED = 2;
    private static final byte FIELD_STRING = 1;
    private static final byte FIELD_LONG = 2;
    private static final byte FIELD_DOUBLE = 3;
    private static final byte FIELD_BOOLEAN = 4;
    private static final byte FIELD_REFERENCE = 5;
    private static final byte IDENTIFIER_RECORD_ID = 1;
    private static final byte IDENTIFIER_STRING = 2;

    private final PageFile pageFile;

    NativeIndexStore(Path path, int pageSize) {
        this.pageFile = new PageFile(path, pageSize);
    }

    void open() throws IOException {
        pageFile.open();
    }

    boolean isOpen() {
        return pageFile.isOpen();
    }

    Set<Object> findExact(byte kind, String namespace, String fieldName, FieldValue fieldValue) {
        try {
            Manifest manifest = readManifest();
            if (manifest == null) {
                return Set.of();
            }
            for (ManifestEntry entry : manifest.entries()) {
                if (entry.kind() == kind
                        && namespace.equals(entry.namespace())
                        && fieldName.equals(entry.fieldName())) {
                    return findExactInDirectoryTree(entry, fieldValue, kind == ENTRY_ORDERED);
                }
            }
            return Set.of();
        } catch (IOException e) {
            throw new RuntimeException("Failed native exact index lookup", e);
        }
    }

    Set<Object> findRange(String namespace, String fieldName, FieldValue fromInclusive, FieldValue toInclusive) {
        try {
            Manifest manifest = readManifest();
            if (manifest == null) {
                return Set.of();
            }
            for (ManifestEntry entry : manifest.entries()) {
                if (entry.kind() == ENTRY_ORDERED
                        && namespace.equals(entry.namespace())
                        && fieldName.equals(entry.fieldName())) {
                    return findRangeInDirectoryTree(entry, fromInclusive, toInclusive);
                }
            }
            return Set.of();
        } catch (IOException e) {
            throw new RuntimeException("Failed native ordered index range lookup", e);
        }
    }

    void loadInto(ExactMatchIndexManager exactManager, OrderedRangeIndexManager orderedManager) {
        exactManager.clear();
        orderedManager.clear();
        try {
            byte[] manifestPayload = pageFile.readPayload();
            if (manifestPayload == null || manifestPayload.length == 0) {
                return;
            }
            Manifest manifest = decodeManifest(manifestPayload);
            Map<String, Map<String, Map<FieldValue, Set<Object>>>> exactFieldSnapshots = new LinkedHashMap<>();
            Map<String, Map<String, Map<FieldValue, Set<Object>>>> orderedFieldSnapshots = new LinkedHashMap<>();
            for (ManifestEntry entry : manifest.entries()) {
                switch (entry.kind()) {
                    case ENTRY_EXACT -> {
                        for (DirectoryValueEntry valueEntry : readDirectoryEntriesFromTree(entry)) {
                            exactFieldSnapshots
                                    .computeIfAbsent(entry.namespace(), ignored -> new LinkedHashMap<>())
                                    .computeIfAbsent(entry.fieldName(), ignored -> new LinkedHashMap<>())
                                    .put(valueEntry.fieldValue(), readIdentifierChain(valueEntry.firstPage(), valueEntry.pageCount()));
                        }
                    }
                    case ENTRY_ORDERED -> {
                        for (DirectoryValueEntry valueEntry : readDirectoryEntriesFromTree(entry)) {
                            orderedFieldSnapshots
                                    .computeIfAbsent(entry.namespace(), ignored -> new LinkedHashMap<>())
                                    .computeIfAbsent(entry.fieldName(), ignored -> new LinkedHashMap<>())
                                    .put(valueEntry.fieldValue(), readIdentifierChain(valueEntry.firstPage(), valueEntry.pageCount()));
                        }
                    }
                    default -> throw new IOException("Unsupported native index store entry kind " + entry.kind());
                }
            }
            for (Map.Entry<String, Map<String, Map<FieldValue, Set<Object>>>> namespaceEntry : exactFieldSnapshots.entrySet()) {
                exactManager.restoreNamespace(namespaceEntry.getKey(), namespaceEntry.getValue());
            }
            for (Map.Entry<String, Map<String, Map<FieldValue, Set<Object>>>> namespaceEntry : orderedFieldSnapshots.entrySet()) {
                orderedManager.restoreNamespace(namespaceEntry.getKey(), namespaceEntry.getValue());
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to load native index store", e);
        }
    }

    void persist(ExactMatchIndexManager exactManager, OrderedRangeIndexManager orderedManager) throws IOException {
        List<ManifestEntry> manifestEntries = new ArrayList<>();
        pageFile.clearEntries();
        pageFile.clearPages();
        for (String namespace : exactManager.namespaces()) {
            for (Map.Entry<String, Map<FieldValue, Set<Object>>> fieldEntry : exactManager.snapshotNamespace(namespace).entrySet()) {
                if (fieldEntry.getValue().isEmpty()) {
                    continue;
                }
                manifestEntries.add(writeFieldDirectoryEntry(ENTRY_EXACT, namespace, fieldEntry.getKey(), fieldEntry.getValue()));
            }
        }
        for (String namespace : orderedManager.namespaces()) {
            for (Map.Entry<String, Map<FieldValue, Set<Object>>> fieldEntry : orderedManager.snapshotNamespace(namespace).entrySet()) {
                if (fieldEntry.getValue().isEmpty()) {
                    continue;
                }
                manifestEntries.add(writeFieldDirectoryEntry(ENTRY_ORDERED, namespace, fieldEntry.getKey(), fieldEntry.getValue()));
            }
        }

        pageFile.writePayload(encodeManifest(new Manifest(manifestEntries)));
    }

    void persistTouchedFields(
            ExactMatchIndexManager exactManager,
            OrderedRangeIndexManager orderedManager,
            Map<String, Set<String>> exactTouchedFields,
            Map<String, Set<String>> orderedTouchedFields
    ) throws IOException {
        Manifest manifest = readManifest();
        Map<ManifestFieldKey, ManifestEntry> updatedEntries = new LinkedHashMap<>();
        if (manifest != null) {
            for (ManifestEntry entry : manifest.entries()) {
                updatedEntries.put(new ManifestFieldKey(entry.kind(), entry.namespace(), entry.fieldName()), entry);
            }
        }
        persistTouchedFieldsForKind(updatedEntries, ENTRY_EXACT, exactManager, exactTouchedFields);
        persistTouchedFieldsForKind(updatedEntries, ENTRY_ORDERED, orderedManager, orderedTouchedFields);
        pageFile.writePayload(encodeManifest(new Manifest(new ArrayList<>(updatedEntries.values()))));
    }

    void applyExactAdd(String namespace, String fieldName, FieldValue fieldValue, Object identifier) throws IOException {
        applyFieldMutation(ENTRY_EXACT, namespace, fieldName, fieldValue, identifier, true);
    }

    void applyExactRemove(String namespace, String fieldName, FieldValue fieldValue, Object identifier) throws IOException {
        applyFieldMutation(ENTRY_EXACT, namespace, fieldName, fieldValue, identifier, false);
    }

    void applyOrderedAdd(String namespace, String fieldName, FieldValue fieldValue, Object identifier) throws IOException {
        applyFieldMutation(ENTRY_ORDERED, namespace, fieldName, fieldValue, identifier, true);
    }

    void applyOrderedRemove(String namespace, String fieldName, FieldValue fieldValue, Object identifier) throws IOException {
        applyFieldMutation(ENTRY_ORDERED, namespace, fieldName, fieldValue, identifier, false);
    }

    void clear() {
        try {
            pageFile.writePayload(new byte[0]);
            pageFile.clearEntries();
            pageFile.clearPages();
        } catch (IOException e) {
            throw new RuntimeException("Failed to clear native index store", e);
        }
    }

    Path path() {
        return pageFile.path();
    }

    DirectoryTreeMetrics describeTree(byte kind, String namespace, String fieldName) throws IOException {
        Manifest manifest = readManifest();
        if (manifest == null) {
            throw new IOException("Missing native index manifest");
        }
        for (ManifestEntry entry : manifest.entries()) {
            if (entry.kind() == kind
                    && namespace.equals(entry.namespace())
                    && fieldName.equals(entry.fieldName())) {
                TreeMetricsAccumulator accumulator = new TreeMetricsAccumulator();
                accumulateTreeMetrics(entry.firstPage(), 1, accumulator);
                return accumulator.toMetrics();
            }
        }
        throw new IOException("Missing directory tree for " + namespace + "." + fieldName);
    }

    List<Integer> describeLeafEntryCounts(byte kind, String namespace, String fieldName) throws IOException {
        Manifest manifest = readManifest();
        if (manifest == null) {
            throw new IOException("Missing native index manifest");
        }
        for (ManifestEntry entry : manifest.entries()) {
            if (entry.kind() == kind
                    && namespace.equals(entry.namespace())
                    && fieldName.equals(entry.fieldName())) {
                List<Integer> counts = new ArrayList<>();
                collectLeafEntryCounts(entry.firstPage(), counts);
                return counts;
            }
        }
        throw new IOException("Missing directory tree for " + namespace + "." + fieldName);
    }

    List<Integer> describeInternalEntryCounts(byte kind, String namespace, String fieldName) throws IOException {
        Manifest manifest = readManifest();
        if (manifest == null) {
            throw new IOException("Missing native index manifest");
        }
        for (ManifestEntry entry : manifest.entries()) {
            if (entry.kind() == kind
                    && namespace.equals(entry.namespace())
                    && fieldName.equals(entry.fieldName())) {
                List<Integer> counts = new ArrayList<>();
                collectInternalEntryCounts(entry.firstPage(), counts);
                return counts;
            }
        }
        throw new IOException("Missing directory tree for " + namespace + "." + fieldName);
    }

    private Manifest readManifest() throws IOException {
        byte[] manifestPayload = pageFile.readPayload();
        if (manifestPayload == null || manifestPayload.length == 0) {
            return null;
        }
        return decodeManifest(manifestPayload);
    }

    private void persistTouchedFieldsForKind(
            Map<ManifestFieldKey, ManifestEntry> updatedEntries,
            byte kind,
            Object manager,
            Map<String, Set<String>> touchedFields
    ) throws IOException {
        for (Map.Entry<String, Set<String>> namespaceEntry : touchedFields.entrySet()) {
            String namespace = namespaceEntry.getKey();
            Map<String, Map<FieldValue, Set<Object>>> fieldSnapshots = switch (kind) {
                case ENTRY_EXACT -> ((ExactMatchIndexManager) manager).snapshotNamespace(namespace);
                case ENTRY_ORDERED -> ((OrderedRangeIndexManager) manager).snapshotNamespace(namespace);
                default -> throw new IOException("Unsupported touched-field persistence kind " + kind);
            };
            for (String fieldName : namespaceEntry.getValue()) {
                ManifestFieldKey key = new ManifestFieldKey(kind, namespace, fieldName);
                Map<FieldValue, Set<Object>> fieldValues = fieldSnapshots.get(fieldName);
                if (fieldValues == null || fieldValues.isEmpty()) {
                    updatedEntries.remove(key);
                    continue;
                }
                updatedEntries.put(
                        key,
                        writeFieldDirectoryEntry(kind, namespace, fieldName, fieldValues)
                );
            }
        }
    }

    private void applyFieldMutation(
            byte kind,
            String namespace,
            String fieldName,
            FieldValue fieldValue,
            Object identifier,
            boolean add
    ) throws IOException {
        Manifest manifest = readManifest();
        if (manifest != null
                && tryApplyInPlaceFieldMutation(manifest, kind, namespace, fieldName, fieldValue, identifier, add)) {
            return;
        }
        List<RewrittenEntry> rewrittenEntries = new ArrayList<>();
        boolean targetHandled = false;
        if (manifest != null) {
            for (ManifestEntry entry : manifest.entries()) {
                if (entry.kind() == kind
                        && namespace.equals(entry.namespace())
                        && fieldName.equals(entry.fieldName())) {
                    List<DirectoryValueEntry> directoryEntries = readDirectoryEntryMetadata(entry);
                    List<DirectoryValueEntry> rewrittenDirectoryEntries = mutateDirectoryEntries(
                            kind,
                            namespace,
                            fieldName,
                            directoryEntries,
                            fieldValue,
                            identifier,
                            add
                    );
                    if (!rewrittenDirectoryEntries.isEmpty()) {
                        rewrittenEntries.add(RewrittenEntry.rebuiltFromDirectoryEntries(kind, namespace, fieldName, rewrittenDirectoryEntries));
                    }
                    targetHandled = true;
                } else {
                    rewrittenEntries.add(RewrittenEntry.rebuiltFromDirectoryEntries(
                            entry.kind(),
                            entry.namespace(),
                            entry.fieldName(),
                            readDirectoryEntryMetadata(entry)
                    ));
                }
            }
        }
        if (!targetHandled && add) {
            ManifestEntry membershipEntry = writeIdentifierChainEntry(kind, namespace, fieldName, fieldValue, new LinkedHashSet<>(Set.of(identifier)));
            rewrittenEntries.add(RewrittenEntry.rebuiltFromDirectoryEntries(
                    kind,
                    namespace,
                    fieldName,
                    List.of(new DirectoryValueEntry(fieldValue, membershipEntry.firstPage(), membershipEntry.pageCount(), membershipEntry.totalLength()))
            ));
        }
        rewriteEntries(rewrittenEntries);
    }

    private boolean tryApplyInPlaceFieldMutation(
            Manifest manifest,
            byte kind,
            String namespace,
            String fieldName,
            FieldValue fieldValue,
            Object identifier,
            boolean add
    ) throws IOException {
        for (ManifestEntry entry : manifest.entries()) {
            if (entry.kind() == kind
                    && namespace.equals(entry.namespace())
                    && fieldName.equals(entry.fieldName())) {
                boolean orderedComparator = kind == ENTRY_ORDERED;
                LeafMutationPath location = locateLeafMutationPath(entry.firstPage(), fieldValue, orderedComparator);
                if (location == null) {
                    return add ? false : true;
                }
                List<DirectoryValueEntry> updatedEntries = new ArrayList<>(location.leafPage().entries());
                int entryIndex = location.entryIndex();
                if (location.existingEntry() != null) {
                    Set<Object> identifiers = readIdentifierChain(
                            location.existingEntry().firstPage(),
                            location.existingEntry().pageCount()
                    );
                    boolean changed = add ? identifiers.add(identifier) : identifiers.remove(identifier);
                    if (!changed) {
                        return true;
                    }
                    if (identifiers.isEmpty()) {
                        updatedEntries.remove(entryIndex);
                    } else {
                        if (!leafEntriesFitAfterMutation(updatedEntries, entryIndex, fieldValue, identifiers.size(), false)) {
                            return false;
                        }
                        ManifestEntry membershipEntry = writeIdentifierChainEntry(
                                kind,
                                namespace,
                                fieldName,
                                fieldValue,
                                identifiers
                        );
                        updatedEntries.set(entryIndex, new DirectoryValueEntry(
                                fieldValue,
                                membershipEntry.firstPage(),
                                membershipEntry.pageCount(),
                                membershipEntry.totalLength()
                        ));
                    }
                } else {
                    if (!add) {
                        return true;
                    }
                    if (!leafEntriesFitAfterMutation(updatedEntries, entryIndex, fieldValue, 1, true)) {
                        return tryApplyLeafSplitInsert(
                                manifest,
                                kind,
                                namespace,
                                fieldName,
                                fieldValue,
                                identifier,
                                location,
                                updatedEntries,
                                entryIndex
                        );
                    }
                    ManifestEntry membershipEntry = writeIdentifierChainEntry(
                            kind,
                            namespace,
                            fieldName,
                            fieldValue,
                            new LinkedHashSet<>(Set.of(identifier))
                    );
                    updatedEntries.add(entryIndex, new DirectoryValueEntry(
                            fieldValue,
                            membershipEntry.firstPage(),
                            membershipEntry.pageCount(),
                            membershipEntry.totalLength()
                    ));
                }
                if (updatedEntries.isEmpty()) {
                    return tryApplyLeafRemoval(
                            manifest,
                            kind,
                            namespace,
                            fieldName,
                            location.ancestors()
                    );
                }
                if (!add && tryApplyLeafSiblingRedistribution(
                        location,
                        updatedEntries
                )) {
                    return true;
                }
                if (!add && tryApplyLeafSiblingMerge(
                        manifest,
                        kind,
                        namespace,
                        fieldName,
                        location,
                        updatedEntries
                )) {
                    return true;
                }
                pageFile.writePage(
                        location.leafPageNo(),
                        encodeDirectoryPage(location.leafPage().nextPage(), updatedEntries)
                );
                rewriteAncestorBounds(
                        location.ancestors(),
                        updatedEntries.get(0).fieldValue(),
                        updatedEntries.get(updatedEntries.size() - 1).fieldValue()
                );
                return true;
            }
        }
        return false;
    }

    private boolean tryApplyLeafSiblingRedistribution(
            LeafMutationPath location,
            List<DirectoryValueEntry> updatedEntries
    ) throws IOException {
        if (location.ancestors().isEmpty()) {
            return false;
        }
        AncestorLocation parent = location.ancestors().get(location.ancestors().size() - 1);
        if (!parent.page().childrenAreLeaves()) {
            return false;
        }

        int childIndex = parent.childIndex();
        List<DirectoryNodeReference> parentEntries = new ArrayList<>(parent.page().entries());

        if (childIndex > 0) {
            DirectoryNodeReference leftSibling = parentEntries.get(childIndex - 1);
            List<DirectoryValueEntry> leftEntries = new ArrayList<>(decodeDirectoryPage(pageFile.readPage(leftSibling.childPage())).entries());
            if (leftEntries.size() > 1) {
                DirectoryValueEntry moved = leftEntries.remove(leftEntries.size() - 1);
                List<DirectoryValueEntry> redistributedCurrent = new ArrayList<>();
                redistributedCurrent.add(moved);
                redistributedCurrent.addAll(updatedEntries);
                if (leafEntriesFit(leftEntries) && leafEntriesFit(redistributedCurrent)) {
                    return rewriteLeafRedistribution(
                            location.ancestors(),
                            childIndex - 1,
                            leftSibling.childPage(),
                            leftEntries,
                            childIndex,
                            location.leafPageNo(),
                            redistributedCurrent
                    );
                }
            }
        }

        if (childIndex + 1 < parentEntries.size()) {
            DirectoryNodeReference rightSibling = parentEntries.get(childIndex + 1);
            List<DirectoryValueEntry> rightEntries = new ArrayList<>(decodeDirectoryPage(pageFile.readPage(rightSibling.childPage())).entries());
            if (rightEntries.size() > 1) {
                DirectoryValueEntry moved = rightEntries.remove(0);
                List<DirectoryValueEntry> redistributedCurrent = new ArrayList<>(updatedEntries);
                redistributedCurrent.add(moved);
                if (leafEntriesFit(rightEntries) && leafEntriesFit(redistributedCurrent)) {
                    return rewriteLeafRedistribution(
                            location.ancestors(),
                            childIndex,
                            location.leafPageNo(),
                            redistributedCurrent,
                            childIndex + 1,
                            rightSibling.childPage(),
                            rightEntries
                    );
                }
            }
        }

        return false;
    }

    private boolean rewriteLeafRedistribution(
            List<AncestorLocation> ancestors,
            int leftChildIndex,
            int leftLeafPageNo,
            List<DirectoryValueEntry> leftEntries,
            int rightChildIndex,
            int rightLeafPageNo,
            List<DirectoryValueEntry> rightEntries
    ) throws IOException {
        pageFile.writePage(leftLeafPageNo, encodeDirectoryPage(-1, leftEntries));
        pageFile.writePage(rightLeafPageNo, encodeDirectoryPage(-1, rightEntries));

        AncestorLocation parent = ancestors.get(ancestors.size() - 1);
        List<DirectoryNodeReference> updatedEntries = new ArrayList<>(parent.page().entries());
        updatedEntries.set(leftChildIndex, new DirectoryNodeReference(
                leftEntries.get(0).fieldValue(),
                leftEntries.get(leftEntries.size() - 1).fieldValue(),
                leftLeafPageNo,
                leftEntries.size()
        ));
        updatedEntries.set(rightChildIndex, new DirectoryNodeReference(
                rightEntries.get(0).fieldValue(),
                rightEntries.get(rightEntries.size() - 1).fieldValue(),
                rightLeafPageNo,
                rightEntries.size()
        ));
        pageFile.writePage(
                parent.pageNo(),
                encodeDirectoryInternalPage(true, updatedEntries)
        );
        if (ancestors.size() > 1) {
            rewriteAncestorBoundsAboveParent(
                    ancestors,
                    updatedEntries.get(0).minFieldValue(),
                    updatedEntries.get(updatedEntries.size() - 1).maxFieldValue()
            );
        }
        return true;
    }

    private boolean tryApplyLeafSiblingMerge(
            Manifest manifest,
            byte kind,
            String namespace,
            String fieldName,
            LeafMutationPath location,
            List<DirectoryValueEntry> updatedEntries
    ) throws IOException {
        if (location.ancestors().isEmpty()) {
            return false;
        }
        AncestorLocation parent = location.ancestors().get(location.ancestors().size() - 1);
        if (!parent.page().childrenAreLeaves()) {
            return false;
        }

        int childIndex = parent.childIndex();
        List<DirectoryNodeReference> parentEntries = new ArrayList<>(parent.page().entries());

        if (childIndex > 0) {
            DirectoryNodeReference leftSibling = parentEntries.get(childIndex - 1);
            List<DirectoryValueEntry> mergedLeft = new ArrayList<>(decodeDirectoryPage(pageFile.readPage(leftSibling.childPage())).entries());
            mergedLeft.addAll(updatedEntries);
            if (leafEntriesFit(mergedLeft)) {
                return mergeLeafIntoSibling(
                        manifest,
                        kind,
                        namespace,
                        fieldName,
                        location.ancestors(),
                        childIndex,
                        leftSibling.childPage(),
                        mergedLeft,
                        childIndex - 1
                );
            }
        }

        if (childIndex + 1 < parentEntries.size()) {
            DirectoryNodeReference rightSibling = parentEntries.get(childIndex + 1);
            List<DirectoryValueEntry> mergedRight = new ArrayList<>(updatedEntries);
            mergedRight.addAll(decodeDirectoryPage(pageFile.readPage(rightSibling.childPage())).entries());
            if (leafEntriesFit(mergedRight)) {
                return mergeLeafIntoSibling(
                        manifest,
                        kind,
                        namespace,
                        fieldName,
                        location.ancestors(),
                        childIndex,
                        location.leafPageNo(),
                        mergedRight,
                        childIndex
                );
            }
        }

        return false;
    }

    private boolean mergeLeafIntoSibling(
            Manifest manifest,
            byte kind,
            String namespace,
            String fieldName,
            List<AncestorLocation> ancestors,
            int removedChildIndex,
            int targetLeafPageNo,
            List<DirectoryValueEntry> mergedEntries,
            int updatedChildIndex
    ) throws IOException {
        pageFile.writePage(targetLeafPageNo, encodeDirectoryPage(-1, mergedEntries));
        return removeChildFromAncestors(
                manifest,
                kind,
                namespace,
                fieldName,
                ancestors,
                ancestors.size() - 1,
                removedChildIndex,
                updatedChildIndex,
                new DirectoryNodeReference(
                        mergedEntries.get(0).fieldValue(),
                        mergedEntries.get(mergedEntries.size() - 1).fieldValue(),
                        targetLeafPageNo,
                        mergedEntries.size()
                )
        );
    }

    private boolean tryApplyLeafRemoval(
            Manifest manifest,
            byte kind,
            String namespace,
            String fieldName,
            List<AncestorLocation> ancestors
    ) throws IOException {
        if (ancestors.isEmpty()) {
            return false;
        }
        for (int level = ancestors.size() - 1; level >= 0; level--) {
            AncestorLocation ancestor = ancestors.get(level);
            List<DirectoryNodeReference> updatedEntries = new ArrayList<>(ancestor.page().entries());
            updatedEntries.remove(ancestor.childIndex());
            if (updatedEntries.isEmpty()) {
                if (level == 0) {
                    removeManifestEntry(manifest, kind, namespace, fieldName);
                    return true;
                }
                continue;
            }
            pageFile.writePage(
                    ancestor.pageNo(),
                    encodeDirectoryInternalPage(ancestor.page().childrenAreLeaves(), updatedEntries)
            );
            if (level > 0) {
                rewriteAncestorBounds(
                        ancestors.subList(0, level),
                        updatedEntries.get(0).minFieldValue(),
                        updatedEntries.get(updatedEntries.size() - 1).maxFieldValue()
                );
            }
            return true;
        }
        return false;
    }

    private boolean tryApplyLeafSplitInsert(
            Manifest manifest,
            byte kind,
            String namespace,
            String fieldName,
            FieldValue fieldValue,
            Object identifier,
            LeafMutationPath location,
            List<DirectoryValueEntry> currentEntries,
            int entryIndex
    ) throws IOException {
        if (location.ancestors().isEmpty()) {
            return false;
        }
        AncestorLocation parent = location.ancestors().get(location.ancestors().size() - 1);
        if (!parent.page().childrenAreLeaves()) {
            return false;
        }

        ManifestEntry membershipEntry = writeIdentifierChainEntry(
                kind,
                namespace,
                fieldName,
                fieldValue,
                new LinkedHashSet<>(Set.of(identifier))
        );

        List<DirectoryValueEntry> insertedEntries = new ArrayList<>(currentEntries);
        insertedEntries.add(entryIndex, new DirectoryValueEntry(
                fieldValue,
                membershipEntry.firstPage(),
                membershipEntry.pageCount(),
                membershipEntry.totalLength()
        ));

        int splitIndex = insertedEntries.size() / 2;
        List<DirectoryValueEntry> leftEntries = new ArrayList<>(insertedEntries.subList(0, splitIndex));
        List<DirectoryValueEntry> rightEntries = new ArrayList<>(insertedEntries.subList(splitIndex, insertedEntries.size()));
        if (leftEntries.isEmpty() || rightEntries.isEmpty()) {
            return false;
        }
        if (encodeDirectoryPage(location.leafPage().nextPage(), leftEntries).length > pageFile.pageSize() - Integer.BYTES
                || encodeDirectoryPage(-1, rightEntries).length > pageFile.pageSize() - Integer.BYTES) {
            return false;
        }

        List<DirectoryNodeReference> parentEntries = new ArrayList<>(parent.page().entries());
        DirectoryNodeReference oldChild = parentEntries.get(parent.childIndex());
        DirectoryNodeReference leftChild = new DirectoryNodeReference(
                leftEntries.get(0).fieldValue(),
                leftEntries.get(leftEntries.size() - 1).fieldValue(),
                oldChild.childPage(),
                leftEntries.size()
        );
        DirectoryNodeReference rightChild = new DirectoryNodeReference(
                rightEntries.get(0).fieldValue(),
                rightEntries.get(rightEntries.size() - 1).fieldValue(),
                -1,
                rightEntries.size()
        );
        int rightLeafPageNo = pageFile.allocatePage();
        rightChild = new DirectoryNodeReference(
                rightChild.minFieldValue(),
                rightChild.maxFieldValue(),
                rightLeafPageNo,
                rightChild.entryCount()
        );

        pageFile.writePage(location.leafPageNo(), encodeDirectoryPage(location.leafPage().nextPage(), leftEntries));
        pageFile.writePage(rightLeafPageNo, encodeDirectoryPage(-1, rightEntries));
        Integer newRootPageNo = propagateSplitToAncestors(location.ancestors(), leftChild, rightChild);
        if (newRootPageNo != null) {
            updateManifestRootPage(manifest, kind, namespace, fieldName, newRootPageNo);
        }
        return true;
    }

    private boolean leafEntriesFitAfterMutation(
            List<DirectoryValueEntry> currentEntries,
            int entryIndex,
            FieldValue fieldValue,
            int identifierCount,
            boolean insert
    ) throws IOException {
        List<DirectoryValueEntry> simulated = new ArrayList<>(currentEntries);
        DirectoryValueEntry placeholder = new DirectoryValueEntry(fieldValue, 0, 1, identifierCount);
        if (insert) {
            simulated.add(entryIndex, placeholder);
        } else {
            simulated.set(entryIndex, placeholder);
        }
        return encodeDirectoryPage(-1, simulated).length <= pageFile.pageSize() - Integer.BYTES;
    }

    private boolean leafEntriesFit(List<DirectoryValueEntry> entries) throws IOException {
        return encodeDirectoryPage(-1, entries).length <= pageFile.pageSize() - Integer.BYTES;
    }

    private void rewriteAncestorBounds(
            List<AncestorLocation> ancestors,
            FieldValue minFieldValue,
            FieldValue maxFieldValue
    ) throws IOException {
        FieldValue currentMin = minFieldValue;
        FieldValue currentMax = maxFieldValue;
        for (int i = ancestors.size() - 1; i >= 0; i--) {
            AncestorLocation ancestor = ancestors.get(i);
            List<DirectoryNodeReference> updatedEntries = new ArrayList<>(ancestor.page().entries());
            DirectoryNodeReference current = updatedEntries.get(ancestor.childIndex());
            updatedEntries.set(ancestor.childIndex(), new DirectoryNodeReference(
                    currentMin,
                    currentMax,
                    current.childPage(),
                    current.entryCount()
            ));
            pageFile.writePage(
                    ancestor.pageNo(),
                    encodeDirectoryInternalPage(ancestor.page().childrenAreLeaves(), updatedEntries)
            );
            currentMin = updatedEntries.get(0).minFieldValue();
            currentMax = updatedEntries.get(updatedEntries.size() - 1).maxFieldValue();
        }
    }

    private void rewriteAncestorBoundsAboveParent(
            List<AncestorLocation> ancestors,
            FieldValue minFieldValue,
            FieldValue maxFieldValue
    ) throws IOException {
        if (ancestors.size() <= 1) {
            return;
        }
        rewriteAncestorBounds(ancestors.subList(0, ancestors.size() - 1), minFieldValue, maxFieldValue);
    }

    private boolean removeChildFromAncestors(
            Manifest manifest,
            byte kind,
            String namespace,
            String fieldName,
            List<AncestorLocation> ancestors,
            int level,
            int removedChildIndex,
            Integer updatedChildIndex,
            DirectoryNodeReference updatedChildReference
    ) throws IOException {
        if (level < 0) {
            return false;
        }
        AncestorLocation ancestor = ancestors.get(level);
        List<DirectoryNodeReference> updatedEntries = new ArrayList<>(ancestor.page().entries());
        updatedEntries.remove(removedChildIndex);
        if (updatedChildIndex != null && updatedChildReference != null) {
            updatedEntries.set(updatedChildIndex, updatedChildReference);
        }

        if (updatedEntries.isEmpty()) {
            if (level == 0) {
                removeManifestEntry(manifest, kind, namespace, fieldName);
                return true;
            }
            return removeChildFromAncestors(
                    manifest,
                    kind,
                    namespace,
                    fieldName,
                    ancestors,
                    level - 1,
                    ancestors.get(level - 1).childIndex(),
                    null,
                null
            );
        }

        if (level > 0 && updatedEntries.size() == 1
                && tryRedistributeInternalSibling(
                ancestors,
                level,
                updatedEntries,
                ancestor.page().childrenAreLeaves()
        )) {
            return true;
        }

        if (updatedEntries.size() == 1 && !ancestor.page().childrenAreLeaves()) {
            DirectoryNodeReference promotedChild = updatedEntries.get(0);
            if (level == 0) {
                updateManifestRootPage(manifest, kind, namespace, fieldName, promotedChild.childPage());
                return true;
            }
            return replaceChildReferenceInAncestors(ancestors, level - 1, promotedChild);
        }

        if (level == 0 && updatedEntries.size() == 1) {
            pageFile.writePage(
                    ancestor.pageNo(),
                    encodeDirectoryInternalPage(ancestor.page().childrenAreLeaves(), updatedEntries)
            );
            return true;
        }

        pageFile.writePage(
                ancestor.pageNo(),
                encodeDirectoryInternalPage(ancestor.page().childrenAreLeaves(), updatedEntries)
        );
        if (level > 0) {
            rewriteAncestorBounds(
                    ancestors.subList(0, level),
                    updatedEntries.get(0).minFieldValue(),
                    updatedEntries.get(updatedEntries.size() - 1).maxFieldValue()
            );
        }
        return true;
    }

    private boolean tryRedistributeInternalSibling(
            List<AncestorLocation> ancestors,
            int level,
            List<DirectoryNodeReference> updatedEntries,
            boolean childrenAreLeaves
    ) throws IOException {
        AncestorLocation parentAncestor = ancestors.get(level - 1);
        int childIndex = parentAncestor.childIndex();
        List<DirectoryNodeReference> parentEntries = new ArrayList<>(parentAncestor.page().entries());

        if (childIndex > 0) {
            DirectoryNodeReference leftSiblingRef = parentEntries.get(childIndex - 1);
            List<DirectoryNodeReference> leftSiblingEntries = new ArrayList<>(
                    decodeDirectoryInternalPage(pageFile.readPage(leftSiblingRef.childPage())).entries()
            );
            if (leftSiblingEntries.size() > 1) {
                DirectoryNodeReference moved = leftSiblingEntries.remove(leftSiblingEntries.size() - 1);
                List<DirectoryNodeReference> redistributedCurrent = new ArrayList<>();
                redistributedCurrent.add(moved);
                redistributedCurrent.addAll(updatedEntries);
                if (internalEntriesFit(childrenAreLeaves, leftSiblingEntries)
                        && internalEntriesFit(childrenAreLeaves, redistributedCurrent)) {
                    return rewriteInternalRedistribution(
                            ancestors,
                            level,
                            childIndex - 1,
                            leftSiblingRef.childPage(),
                            leftSiblingEntries,
                            childIndex,
                            ancestors.get(level).pageNo(),
                            redistributedCurrent,
                            childrenAreLeaves
                    );
                }
            }
        }

        if (childIndex + 1 < parentEntries.size()) {
            DirectoryNodeReference rightSiblingRef = parentEntries.get(childIndex + 1);
            List<DirectoryNodeReference> rightSiblingEntries = new ArrayList<>(
                    decodeDirectoryInternalPage(pageFile.readPage(rightSiblingRef.childPage())).entries()
            );
            if (rightSiblingEntries.size() > 1) {
                DirectoryNodeReference moved = rightSiblingEntries.remove(0);
                List<DirectoryNodeReference> redistributedCurrent = new ArrayList<>(updatedEntries);
                redistributedCurrent.add(moved);
                if (internalEntriesFit(childrenAreLeaves, rightSiblingEntries)
                        && internalEntriesFit(childrenAreLeaves, redistributedCurrent)) {
                    return rewriteInternalRedistribution(
                            ancestors,
                            level,
                            childIndex,
                            ancestors.get(level).pageNo(),
                            redistributedCurrent,
                            childIndex + 1,
                            rightSiblingRef.childPage(),
                            rightSiblingEntries,
                            childrenAreLeaves
                    );
                }
            }
        }
        return false;
    }

    private boolean rewriteInternalRedistribution(
            List<AncestorLocation> ancestors,
            int level,
            int leftChildIndex,
            int leftPageNo,
            List<DirectoryNodeReference> leftEntries,
            int rightChildIndex,
            int rightPageNo,
            List<DirectoryNodeReference> rightEntries,
            boolean childrenAreLeaves
    ) throws IOException {
        pageFile.writePage(leftPageNo, encodeDirectoryInternalPage(childrenAreLeaves, leftEntries));
        pageFile.writePage(rightPageNo, encodeDirectoryInternalPage(childrenAreLeaves, rightEntries));

        AncestorLocation parentAncestor = ancestors.get(level - 1);
        List<DirectoryNodeReference> updatedParentEntries = new ArrayList<>(parentAncestor.page().entries());
        updatedParentEntries.set(leftChildIndex, buildInternalReference(leftPageNo, leftEntries));
        updatedParentEntries.set(rightChildIndex, buildInternalReference(rightPageNo, rightEntries));
        pageFile.writePage(
                parentAncestor.pageNo(),
                encodeDirectoryInternalPage(parentAncestor.page().childrenAreLeaves(), updatedParentEntries)
        );
        if (level - 1 > 0) {
            rewriteAncestorBounds(
                    ancestors.subList(0, level - 1),
                    updatedParentEntries.get(0).minFieldValue(),
                    updatedParentEntries.get(updatedParentEntries.size() - 1).maxFieldValue()
            );
        }
        return true;
    }

    private boolean replaceChildReferenceInAncestors(
            List<AncestorLocation> ancestors,
            int level,
            DirectoryNodeReference replacement
    ) throws IOException {
        AncestorLocation ancestor = ancestors.get(level);
        List<DirectoryNodeReference> updatedEntries = new ArrayList<>(ancestor.page().entries());
        updatedEntries.set(ancestor.childIndex(), replacement);
        pageFile.writePage(
                ancestor.pageNo(),
                encodeDirectoryInternalPage(ancestor.page().childrenAreLeaves(), updatedEntries)
        );
        if (level > 0) {
            rewriteAncestorBounds(
                    ancestors.subList(0, level),
                    updatedEntries.get(0).minFieldValue(),
                    updatedEntries.get(updatedEntries.size() - 1).maxFieldValue()
            );
        }
        return true;
    }

    private Integer propagateSplitToAncestors(
            List<AncestorLocation> ancestors,
            DirectoryNodeReference leftChild,
            DirectoryNodeReference rightChild
    ) throws IOException {
        DirectoryNodeReference promotedLeft = leftChild;
        DirectoryNodeReference promotedRight = rightChild;

        for (int level = ancestors.size() - 1; level >= 0; level--) {
            AncestorLocation ancestor = ancestors.get(level);
            List<DirectoryNodeReference> updatedEntries = new ArrayList<>(ancestor.page().entries());
            updatedEntries.set(ancestor.childIndex(), promotedLeft);
            updatedEntries.add(ancestor.childIndex() + 1, promotedRight);

            if (internalEntriesFit(ancestor.page().childrenAreLeaves(), updatedEntries)) {
                pageFile.writePage(
                        ancestor.pageNo(),
                        encodeDirectoryInternalPage(ancestor.page().childrenAreLeaves(), updatedEntries)
                );
                if (level > 0) {
                    rewriteAncestorBounds(
                            ancestors.subList(0, level),
                            updatedEntries.get(0).minFieldValue(),
                            updatedEntries.get(updatedEntries.size() - 1).maxFieldValue()
                    );
                }
                return null;
            }

            InternalPageSplit split = splitInternalPage(
                    ancestor.page().childrenAreLeaves(),
                    ancestor.pageNo(),
                    updatedEntries
            );
            if (split == null) {
                return null;
            }
            promotedLeft = split.leftReference();
            promotedRight = split.rightReference();
        }

        int newRootPageNo = pageFile.allocatePage();
        List<DirectoryNodeReference> rootEntries = List.of(promotedLeft, promotedRight);
        pageFile.writePage(newRootPageNo, encodeDirectoryInternalPage(false, rootEntries));
        return newRootPageNo;
    }

    private boolean internalEntriesFit(boolean childrenAreLeaves, List<DirectoryNodeReference> entries) throws IOException {
        return encodeDirectoryInternalPage(childrenAreLeaves, entries).length <= pageFile.pageSize() - Integer.BYTES;
    }

    private InternalPageSplit splitInternalPage(
            boolean childrenAreLeaves,
            int existingPageNo,
            List<DirectoryNodeReference> entries
    ) throws IOException {
        List<List<DirectoryNodeReference>> chunks = chunkDirectoryNodeReferences(entries);
        if (chunks.size() != 2 || chunks.get(0).isEmpty() || chunks.get(1).isEmpty()) {
            return null;
        }

        List<DirectoryNodeReference> leftEntries = chunks.get(0);
        List<DirectoryNodeReference> rightEntries = chunks.get(1);
        int rightPageNo = pageFile.allocatePage();

        pageFile.writePage(existingPageNo, encodeDirectoryInternalPage(childrenAreLeaves, leftEntries));
        pageFile.writePage(rightPageNo, encodeDirectoryInternalPage(childrenAreLeaves, rightEntries));

        return new InternalPageSplit(
                buildInternalReference(existingPageNo, leftEntries),
                buildInternalReference(rightPageNo, rightEntries)
        );
    }

    private DirectoryNodeReference buildInternalReference(int childPage, List<DirectoryNodeReference> entries) {
        int entryCount = 0;
        for (DirectoryNodeReference entry : entries) {
            entryCount += entry.entryCount();
        }
        return new DirectoryNodeReference(
                entries.get(0).minFieldValue(),
                entries.get(entries.size() - 1).maxFieldValue(),
                childPage,
                entryCount
        );
    }

    private void updateManifestRootPage(
            Manifest manifest,
            byte kind,
            String namespace,
            String fieldName,
            int newRootPageNo
    ) throws IOException {
        List<ManifestEntry> updatedEntries = new ArrayList<>(manifest.entries().size());
        boolean replaced = false;
        for (ManifestEntry entry : manifest.entries()) {
            if (entry.kind() == kind
                    && namespace.equals(entry.namespace())
                    && fieldName.equals(entry.fieldName())) {
                updatedEntries.add(new ManifestEntry(
                        entry.kind(),
                        entry.namespace(),
                        entry.fieldName(),
                        entry.fieldValue(),
                        newRootPageNo,
                        entry.pageCount(),
                        entry.totalLength()
                ));
                replaced = true;
            } else {
                updatedEntries.add(entry);
            }
        }
        if (!replaced) {
            throw new IOException("Missing manifest entry for " + namespace + "." + fieldName);
        }
        pageFile.writePayload(encodeManifest(new Manifest(updatedEntries)));
    }

    private void removeManifestEntry(
            Manifest manifest,
            byte kind,
            String namespace,
            String fieldName
    ) throws IOException {
        List<ManifestEntry> updatedEntries = new ArrayList<>(manifest.entries().size());
        boolean removed = false;
        for (ManifestEntry entry : manifest.entries()) {
            if (entry.kind() == kind
                    && namespace.equals(entry.namespace())
                    && fieldName.equals(entry.fieldName())) {
                removed = true;
            } else {
                updatedEntries.add(entry);
            }
        }
        if (!removed) {
            throw new IOException("Missing manifest entry for " + namespace + "." + fieldName);
        }
        pageFile.writePayload(encodeManifest(new Manifest(updatedEntries)));
    }

    private void rewriteEntries(List<RewrittenEntry> rewrittenEntries) throws IOException {
        List<FieldValueRewrite> fieldRewrites = new ArrayList<>();
        for (RewrittenEntry rewrite : rewrittenEntries) {
            fieldRewrites.add(new FieldValueRewrite(
                    rewrite.kind(),
                    rewrite.namespace(),
                    rewrite.fieldName(),
                    snapshotFieldValues(rewrite.directoryEntries())
            ));
        }
        pageFile.clearPages();
        List<ManifestEntry> manifestEntries = new ArrayList<>();
        for (FieldValueRewrite rewrite : fieldRewrites) {
            manifestEntries.add(writeFieldDirectoryEntryFromDirectoryEntries(
                    rewrite.kind(),
                    rewrite.namespace(),
                    rewrite.fieldName(),
                    buildDirectoryEntriesFromFieldValues(rewrite.kind(), rewrite.namespace(), rewrite.fieldName(), rewrite.fieldValues())
            ));
        }
        pageFile.writePayload(encodeManifest(new Manifest(manifestEntries)));
    }

    private Map<FieldValue, Set<Object>> snapshotFieldValues(List<DirectoryValueEntry> directoryEntries) throws IOException {
        Map<FieldValue, Set<Object>> fieldValues = new LinkedHashMap<>();
        for (DirectoryValueEntry entry : directoryEntries) {
            fieldValues.put(entry.fieldValue(), readIdentifierChain(entry.firstPage(), entry.pageCount()));
        }
        return fieldValues;
    }

    private List<DirectoryValueEntry> buildDirectoryEntriesFromFieldValues(
            byte kind,
            String namespace,
            String fieldName,
            Map<FieldValue, Set<Object>> fieldValues
    ) throws IOException {
        List<DirectoryValueEntry> directoryEntries = new ArrayList<>();
        for (Map.Entry<FieldValue, Set<Object>> valueEntry : fieldValues.entrySet()) {
            ManifestEntry membershipEntry = writeIdentifierChainEntry(kind, namespace, fieldName, valueEntry.getKey(), valueEntry.getValue());
            directoryEntries.add(new DirectoryValueEntry(
                    valueEntry.getKey(),
                    membershipEntry.firstPage(),
                    membershipEntry.pageCount(),
                    membershipEntry.totalLength()
            ));
        }
        if (kind == ENTRY_ORDERED) {
            directoryEntries.sort((left, right) -> OrderedRangeIndex.compare(left.fieldValue(), right.fieldValue()));
        } else {
            directoryEntries.sort((left, right) -> compareExactFieldValue(left.fieldValue(), right.fieldValue()));
        }
        return directoryEntries;
    }

    private List<DirectoryValueEntry> mutateDirectoryEntries(
            byte kind,
            String namespace,
            String fieldName,
            List<DirectoryValueEntry> directoryEntries,
            FieldValue fieldValue,
            Object identifier,
            boolean add
    ) throws IOException {
        List<DirectoryValueEntry> rewritten = new ArrayList<>();
        boolean targetFound = false;
        for (DirectoryValueEntry directoryEntry : directoryEntries) {
            if (directoryEntry.fieldValue().equals(fieldValue)) {
                Set<Object> identifiers = readIdentifierChain(directoryEntry.firstPage(), directoryEntry.pageCount());
                if (add) {
                    identifiers.add(identifier);
                } else {
                    identifiers.remove(identifier);
                }
                if (!identifiers.isEmpty()) {
                    ManifestEntry membershipEntry = writeIdentifierChainEntry(kind, namespace, fieldName, fieldValue, identifiers);
                    rewritten.add(new DirectoryValueEntry(
                            fieldValue,
                            membershipEntry.firstPage(),
                            membershipEntry.pageCount(),
                            membershipEntry.totalLength()
                    ));
                }
                targetFound = true;
            } else {
                rewritten.add(directoryEntry);
            }
        }
        if (!targetFound && add) {
            Set<Object> identifiers = new LinkedHashSet<>();
            identifiers.add(identifier);
            ManifestEntry membershipEntry = writeIdentifierChainEntry(kind, namespace, fieldName, fieldValue, identifiers);
            rewritten.add(new DirectoryValueEntry(
                    fieldValue,
                    membershipEntry.firstPage(),
                    membershipEntry.pageCount(),
                    membershipEntry.totalLength()
            ));
        }
        if (kind == ENTRY_ORDERED) {
            rewritten.sort((left, right) -> OrderedRangeIndex.compare(left.fieldValue(), right.fieldValue()));
        } else {
            rewritten.sort((left, right) -> compareExactFieldValue(left.fieldValue(), right.fieldValue()));
        }
        return rewritten;
    }

    private Map<FieldValue, Set<Object>> readFieldValueMap(ManifestEntry entry) throws IOException {
        Map<FieldValue, Set<Object>> fieldValues = new LinkedHashMap<>();
        for (DirectoryValueEntry valueEntry : readDirectoryEntriesFromTree(entry)) {
            fieldValues.put(valueEntry.fieldValue(), readIdentifierChain(valueEntry.firstPage(), valueEntry.pageCount()));
        }
        return fieldValues;
    }

    private List<DirectoryValueEntry> readDirectoryEntryMetadata(ManifestEntry entry) throws IOException {
        return readDirectoryEntriesFromTree(entry);
    }

    private byte[] encodeManifest(Manifest manifest) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(buffer);
        out.writeInt(manifest.version());
        out.writeInt(manifest.entries().size());
        for (ManifestEntry entry : manifest.entries()) {
            out.writeByte(entry.kind());
            out.writeUTF(entry.namespace());
            out.writeBoolean(entry.fieldName() != null);
            if (entry.fieldName() != null) {
                out.writeUTF(entry.fieldName());
            }
            out.writeBoolean(entry.fieldValue() != null);
            if (entry.fieldValue() != null) {
                writeFieldValue(out, entry.fieldValue());
            }
            out.writeInt(entry.firstPage());
            out.writeInt(entry.pageCount());
            out.writeInt(entry.totalLength());
        }
        out.flush();
        return buffer.toByteArray();
    }

    private Manifest decodeManifest(byte[] payload) throws IOException {
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(payload));
        int version = in.readInt();
        if (version != VERSION) {
            throw new IOException("Unsupported native index store manifest version " + version);
        }
        int entryCount = in.readInt();
        List<ManifestEntry> entries = new ArrayList<>(entryCount);
        for (int i = 0; i < entryCount; i++) {
            byte kind = in.readByte();
            String namespace = in.readUTF();
            String fieldName = in.readBoolean() ? in.readUTF() : null;
            FieldValue fieldValue = in.readBoolean() ? readFieldValue(in) : null;
            entries.add(new ManifestEntry(kind, namespace, fieldName, fieldValue, in.readInt(), in.readInt(), in.readInt()));
        }
        return new Manifest(version, entries);
    }

    private ManifestEntry writeIdentifierChainEntry(
            byte kind,
            String namespace,
            String fieldName,
            FieldValue fieldValue,
            Set<Object> identifiers
    ) throws IOException {
        int pageCount = 0;
        int firstPage = -1;
        int previousPage = -1;
        for (Object identifier : identifiers) {
            int pageNo = pageFile.allocatePage();
            if (firstPage < 0) {
                firstPage = pageNo;
            }
            byte[] payload = encodeMembershipPage(-1, identifier);
            pageFile.writePage(pageNo, payload);
            if (previousPage >= 0) {
                rewriteMembershipNextPage(previousPage, pageNo);
            }
            previousPage = pageNo;
            pageCount += 1;
        }
        return new ManifestEntry(kind, namespace, fieldName, fieldValue, firstPage, pageCount, identifiers.size());
    }

    private ManifestEntry writeFieldDirectoryEntry(
            byte kind,
            String namespace,
            String fieldName,
            Map<FieldValue, Set<Object>> fieldValues
    ) throws IOException {
        return writeFieldDirectoryEntryFromDirectoryEntries(
                kind,
                namespace,
                fieldName,
                buildDirectoryEntriesFromFieldValues(kind, namespace, fieldName, fieldValues)
        );
    }

    private ManifestEntry writeFieldDirectoryEntryFromDirectoryEntries(
            byte kind,
            String namespace,
            String fieldName,
            List<DirectoryValueEntry> directoryEntries
    ) throws IOException {
        return writeFieldDirectoryPages(kind, namespace, fieldName, directoryEntries);
    }

    private ManifestEntry writeFieldDirectoryPages(
            byte kind,
            String namespace,
            String fieldName,
            List<DirectoryValueEntry> directoryEntries
    ) throws IOException {
        if (directoryEntries.isEmpty()) {
            throw new IOException("Cannot build native index tree for empty field " + namespace + "." + fieldName);
        }
        List<List<DirectoryValueEntry>> chunks = chunkDirectoryEntries(directoryEntries);
        List<DirectoryNodeReference> leafReferences = new ArrayList<>();
        int totalPages = 0;
        for (List<DirectoryValueEntry> chunk : chunks) {
            int leafPage = pageFile.allocatePage();
            pageFile.writePage(leafPage, encodeDirectoryPage(-1, chunk));
            leafReferences.add(new DirectoryNodeReference(
                    chunk.get(0).fieldValue(),
                    chunk.get(chunk.size() - 1).fieldValue(),
                    leafPage,
                    chunk.size()
            ));
            totalPages += 1;
        }
        TreeBuildResult tree = buildDirectoryTree(leafReferences);
        return new ManifestEntry(kind, namespace, fieldName, null, tree.rootPage(), totalPages + tree.internalPageCount(), directoryEntries.size());
    }

    private Set<Object> readIdentifierChain(ManifestEntry entry) throws IOException {
        return readIdentifierChain(entry.firstPage(), entry.pageCount());
    }

    private Set<Object> readIdentifierChain(int firstPage, int pageCount) throws IOException {
        Set<Object> identifiers = new LinkedHashSet<>();
        int currentPage = firstPage;
        for (int i = 0; i < pageCount && currentPage >= 0; i++) {
            MembershipPage membershipPage = decodeMembershipPage(pageFile.readPage(currentPage));
            identifiers.add(membershipPage.identifier());
            currentPage = membershipPage.nextPage();
        }
        return identifiers;
    }

    private Set<Object> findExactInDirectoryTree(ManifestEntry entry, FieldValue target, boolean orderedComparator) throws IOException {
        return findExactInInternalPage(entry.firstPage(), target, orderedComparator);
    }

    private Set<Object> findRangeInDirectoryTree(ManifestEntry entry, FieldValue fromInclusive, FieldValue toInclusive) throws IOException {
        return findRangeInInternalPage(entry.firstPage(), fromInclusive, toInclusive);
    }

    private List<DirectoryValueEntry> readDirectoryEntriesFromTree(ManifestEntry entry) throws IOException {
        List<DirectoryValueEntry> entries = new ArrayList<>();
        collectDirectoryEntriesFromInternalPage(entry.firstPage(), entries);
        return entries;
    }

    private byte[] encodeDirectoryPage(int nextPage, List<DirectoryValueEntry> directoryEntries) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(buffer);
        out.writeInt(nextPage);
        out.writeInt(directoryEntries.size());
        writeFieldValue(out, directoryEntries.get(0).fieldValue());
        writeFieldValue(out, directoryEntries.get(directoryEntries.size() - 1).fieldValue());
        for (DirectoryValueEntry entry : directoryEntries) {
            writeFieldValue(out, entry.fieldValue());
            out.writeInt(entry.firstPage());
            out.writeInt(entry.pageCount());
            out.writeInt(entry.identifierCount());
        }
        out.flush();
        return buffer.toByteArray();
    }

    private byte[] encodeDirectorySummaryPage(int nextPage, List<DirectoryLeafReference> entries) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(buffer);
        out.writeInt(nextPage);
        out.writeInt(entries.size());
        writeFieldValue(out, entries.get(0).minFieldValue());
        writeFieldValue(out, entries.get(entries.size() - 1).maxFieldValue());
        for (DirectoryLeafReference entry : entries) {
            writeFieldValue(out, entry.minFieldValue());
            writeFieldValue(out, entry.maxFieldValue());
            out.writeInt(entry.firstPage());
            out.writeInt(entry.pageCount());
            out.writeInt(entry.entryCount());
        }
        out.flush();
        return buffer.toByteArray();
    }

    private byte[] encodeDirectoryInternalPage(boolean childrenAreLeaves, List<DirectoryNodeReference> entries) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(buffer);
        out.writeBoolean(childrenAreLeaves);
        out.writeInt(entries.size());
        writeFieldValue(out, entries.get(0).minFieldValue());
        writeFieldValue(out, entries.get(entries.size() - 1).maxFieldValue());
        for (DirectoryNodeReference entry : entries) {
            writeFieldValue(out, entry.minFieldValue());
            writeFieldValue(out, entry.maxFieldValue());
            out.writeInt(entry.childPage());
            out.writeInt(entry.entryCount());
        }
        out.flush();
        return buffer.toByteArray();
    }

    private DirectoryPage decodeDirectoryPage(byte[] payload) throws IOException {
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(payload));
        int nextPage = in.readInt();
        int entryCount = in.readInt();
        FieldValue minFieldValue = readFieldValue(in);
        FieldValue maxFieldValue = readFieldValue(in);
        List<DirectoryValueEntry> entries = new ArrayList<>(entryCount);
        for (int i = 0; i < entryCount; i++) {
            entries.add(new DirectoryValueEntry(
                    readFieldValue(in),
                    in.readInt(),
                    in.readInt(),
                    in.readInt()
            ));
        }
        return new DirectoryPage(nextPage, minFieldValue, maxFieldValue, entries);
    }

    private DirectorySummaryPage decodeDirectorySummaryPage(byte[] payload) throws IOException {
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(payload));
        int nextPage = in.readInt();
        int entryCount = in.readInt();
        FieldValue minFieldValue = readFieldValue(in);
        FieldValue maxFieldValue = readFieldValue(in);
        List<DirectoryLeafReference> entries = new ArrayList<>(entryCount);
        for (int i = 0; i < entryCount; i++) {
            entries.add(new DirectoryLeafReference(
                    readFieldValue(in),
                    readFieldValue(in),
                    in.readInt(),
                    in.readInt(),
                    in.readInt()
            ));
        }
        return new DirectorySummaryPage(nextPage, minFieldValue, maxFieldValue, entries);
    }

    private DirectoryInternalPage decodeDirectoryInternalPage(byte[] payload) throws IOException {
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(payload));
        boolean childrenAreLeaves = in.readBoolean();
        int entryCount = in.readInt();
        FieldValue minFieldValue = readFieldValue(in);
        FieldValue maxFieldValue = readFieldValue(in);
        List<DirectoryNodeReference> entries = new ArrayList<>(entryCount);
        for (int i = 0; i < entryCount; i++) {
            entries.add(new DirectoryNodeReference(
                    readFieldValue(in),
                    readFieldValue(in),
                    in.readInt(),
                    in.readInt()
            ));
        }
        return new DirectoryInternalPage(childrenAreLeaves, minFieldValue, maxFieldValue, entries);
    }

    private List<List<DirectoryValueEntry>> chunkDirectoryEntries(List<DirectoryValueEntry> directoryEntries) throws IOException {
        int capacity = pageFile.pageSize() - Integer.BYTES;
        List<List<DirectoryValueEntry>> chunks = new ArrayList<>();
        List<DirectoryValueEntry> currentChunk = new ArrayList<>();
        for (DirectoryValueEntry entry : directoryEntries) {
            List<DirectoryValueEntry> candidate = new ArrayList<>(currentChunk);
            candidate.add(entry);
            if (!currentChunk.isEmpty() && encodeDirectoryPage(-1, candidate).length > capacity) {
                chunks.add(currentChunk);
                currentChunk = new ArrayList<>();
            }
            currentChunk.add(entry);
        }
        if (!currentChunk.isEmpty()) {
            chunks.add(currentChunk);
        }
        return rebalanceDirectoryValueChunks(chunks);
    }

    private List<List<DirectoryLeafReference>> chunkDirectoryLeafReferences(List<DirectoryLeafReference> leafReferences) throws IOException {
        int capacity = pageFile.pageSize() - Integer.BYTES;
        List<List<DirectoryLeafReference>> chunks = new ArrayList<>();
        List<DirectoryLeafReference> currentChunk = new ArrayList<>();
        for (DirectoryLeafReference entry : leafReferences) {
            List<DirectoryLeafReference> candidate = new ArrayList<>(currentChunk);
            candidate.add(entry);
            if (!currentChunk.isEmpty() && encodeDirectorySummaryPage(-1, candidate).length > capacity) {
                chunks.add(currentChunk);
                currentChunk = new ArrayList<>();
            }
            currentChunk.add(entry);
        }
        if (!currentChunk.isEmpty()) {
            chunks.add(currentChunk);
        }
        return rebalanceDirectoryLeafChunks(chunks);
    }

    private List<List<DirectoryNodeReference>> chunkDirectoryNodeReferences(List<DirectoryNodeReference> references) throws IOException {
        int capacity = pageFile.pageSize() - Integer.BYTES;
        List<List<DirectoryNodeReference>> chunks = new ArrayList<>();
        List<DirectoryNodeReference> currentChunk = new ArrayList<>();
        for (DirectoryNodeReference entry : references) {
            List<DirectoryNodeReference> candidate = new ArrayList<>(currentChunk);
            candidate.add(entry);
            if (!currentChunk.isEmpty() && encodeDirectoryInternalPage(true, candidate).length > capacity) {
                chunks.add(currentChunk);
                currentChunk = new ArrayList<>();
            }
            currentChunk.add(entry);
        }
        if (!currentChunk.isEmpty()) {
            chunks.add(currentChunk);
        }
        return rebalanceDirectoryNodeChunks(chunks);
    }

    private List<List<DirectoryValueEntry>> rebalanceDirectoryValueChunks(List<List<DirectoryValueEntry>> chunks) throws IOException {
        while (chunks.size() > 1) {
            List<DirectoryValueEntry> previous = chunks.get(chunks.size() - 2);
            List<DirectoryValueEntry> last = chunks.get(chunks.size() - 1);
            if (last.size() >= previous.size() - 1 || previous.size() <= 1) {
                break;
            }
            DirectoryValueEntry moved = previous.remove(previous.size() - 1);
            last.add(0, moved);
            if (encodeDirectoryPage(-1, previous).length > pageFile.pageSize() - Integer.BYTES
                    || encodeDirectoryPage(-1, last).length > pageFile.pageSize() - Integer.BYTES) {
                last.remove(0);
                previous.add(moved);
                break;
            }
        }
        return chunks;
    }

    private List<List<DirectoryLeafReference>> rebalanceDirectoryLeafChunks(List<List<DirectoryLeafReference>> chunks) throws IOException {
        while (chunks.size() > 1) {
            List<DirectoryLeafReference> previous = chunks.get(chunks.size() - 2);
            List<DirectoryLeafReference> last = chunks.get(chunks.size() - 1);
            if (last.size() >= previous.size() - 1 || previous.size() <= 1) {
                break;
            }
            DirectoryLeafReference moved = previous.remove(previous.size() - 1);
            last.add(0, moved);
            if (encodeDirectorySummaryPage(-1, previous).length > pageFile.pageSize() - Integer.BYTES
                    || encodeDirectorySummaryPage(-1, last).length > pageFile.pageSize() - Integer.BYTES) {
                last.remove(0);
                previous.add(moved);
                break;
            }
        }
        return chunks;
    }

    private List<List<DirectoryNodeReference>> rebalanceDirectoryNodeChunks(List<List<DirectoryNodeReference>> chunks) throws IOException {
        while (chunks.size() > 1) {
            List<DirectoryNodeReference> previous = chunks.get(chunks.size() - 2);
            List<DirectoryNodeReference> last = chunks.get(chunks.size() - 1);
            if (last.size() >= previous.size() - 1 || previous.size() <= 1) {
                break;
            }
            DirectoryNodeReference moved = previous.remove(previous.size() - 1);
            last.add(0, moved);
            if (encodeDirectoryInternalPage(true, previous).length > pageFile.pageSize() - Integer.BYTES
                    || encodeDirectoryInternalPage(true, last).length > pageFile.pageSize() - Integer.BYTES) {
                last.remove(0);
                previous.add(moved);
                break;
            }
        }
        return chunks;
    }

    private void rewriteDirectoryNextPage(int pageNo, int nextPage) throws IOException {
        DirectoryPage current = decodeDirectoryPage(pageFile.readPage(pageNo));
        pageFile.writePage(pageNo, encodeDirectoryPage(nextPage, current.entries()));
    }

    private void rewriteDirectorySummaryNextPage(int pageNo, int nextPage) throws IOException {
        DirectorySummaryPage current = decodeDirectorySummaryPage(pageFile.readPage(pageNo));
        pageFile.writePage(pageNo, encodeDirectorySummaryPage(nextPage, current.entries()));
    }

    private TreeBuildResult buildDirectoryTree(List<DirectoryNodeReference> leafReferences) throws IOException {
        List<DirectoryNodeReference> currentLevel = leafReferences;
        boolean childrenAreLeaves = true;
        int internalPageCount = 0;
        while (true) {
            List<List<DirectoryNodeReference>> chunks = chunkDirectoryNodeReferences(currentLevel);
            List<DirectoryNodeReference> parentLevel = new ArrayList<>();
            for (List<DirectoryNodeReference> chunk : chunks) {
                int pageNo = pageFile.allocatePage();
                pageFile.writePage(pageNo, encodeDirectoryInternalPage(childrenAreLeaves, chunk));
                int entryCount = 0;
                for (DirectoryNodeReference entry : chunk) {
                    entryCount += entry.entryCount();
                }
                parentLevel.add(new DirectoryNodeReference(
                        chunk.get(0).minFieldValue(),
                        chunk.get(chunk.size() - 1).maxFieldValue(),
                        pageNo,
                        entryCount
                ));
                internalPageCount += 1;
            }
            if (parentLevel.size() == 1) {
                return new TreeBuildResult(parentLevel.get(0).childPage(), internalPageCount);
            }
            currentLevel = parentLevel;
            childrenAreLeaves = false;
        }
    }

    private int findExactDirectoryIndex(List<DirectoryValueEntry> directory, FieldValue target) {
        int low = 0;
        int high = directory.size() - 1;
        while (low <= high) {
            int mid = (low + high) >>> 1;
            int compare = compareExactFieldValue(directory.get(mid).fieldValue(), target);
            if (compare < 0) {
                low = mid + 1;
            } else if (compare > 0) {
                high = mid - 1;
            } else {
                return mid;
            }
        }
        return -1;
    }

    private int compareDirectoryValue(FieldValue left, FieldValue right, boolean orderedComparator) {
        return orderedComparator
                ? OrderedRangeIndex.compare(left, right)
                : compareExactFieldValue(left, right);
    }

    private int compareExactFieldValue(FieldValue left, FieldValue right) {
        if (left instanceof StringValue leftString && right instanceof StringValue rightString) {
            return leftString.value().compareTo(rightString.value());
        }
        if (left instanceof LongValue leftLong && right instanceof LongValue rightLong) {
            return Long.compare(leftLong.value(), rightLong.value());
        }
        if (left instanceof DoubleValue leftDouble && right instanceof DoubleValue rightDouble) {
            return Double.compare(leftDouble.value(), rightDouble.value());
        }
        if (left instanceof BooleanValue leftBoolean && right instanceof BooleanValue rightBoolean) {
            return Boolean.compare(leftBoolean.value(), rightBoolean.value());
        }
        if (left instanceof ReferenceValue leftReference && right instanceof ReferenceValue rightReference) {
            return Long.compare(leftReference.recordId().value(), rightReference.recordId().value());
        }
        return Integer.compare(fieldKindOrder(left), fieldKindOrder(right));
    }

    private int fieldKindOrder(FieldValue value) {
        if (value instanceof StringValue) {
            return 1;
        }
        if (value instanceof LongValue) {
            return 2;
        }
        if (value instanceof DoubleValue) {
            return 3;
        }
        if (value instanceof BooleanValue) {
            return 4;
        }
        if (value instanceof ReferenceValue) {
            return 5;
        }
        throw new IllegalArgumentException("Unsupported exact field value: " + value);
    }

    private int lowerBoundDirectoryIndex(List<DirectoryValueEntry> directory, FieldValue target) {
        int low = 0;
        int high = directory.size();
        while (low < high) {
            int mid = (low + high) >>> 1;
            if (OrderedRangeIndex.compare(directory.get(mid).fieldValue(), target) < 0) {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
        return low;
    }

    private int upperBoundDirectoryIndex(List<DirectoryValueEntry> directory, FieldValue target) {
        int low = 0;
        int high = directory.size();
        while (low < high) {
            int mid = (low + high) >>> 1;
            if (OrderedRangeIndex.compare(directory.get(mid).fieldValue(), target) <= 0) {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
        return low;
    }

    private int findSummaryExactIndex(List<DirectoryLeafReference> directory, FieldValue target, boolean orderedComparator) {
        int low = 0;
        int high = directory.size() - 1;
        while (low <= high) {
            int mid = (low + high) >>> 1;
            DirectoryLeafReference entry = directory.get(mid);
            if (compareDirectoryValue(entry.minFieldValue(), target, orderedComparator) > 0) {
                high = mid - 1;
            } else if (compareDirectoryValue(entry.maxFieldValue(), target, orderedComparator) < 0) {
                low = mid + 1;
            } else {
                return mid;
            }
        }
        return -1;
    }

    private int findNodeExactIndex(List<DirectoryNodeReference> directory, FieldValue target, boolean orderedComparator) {
        int low = 0;
        int high = directory.size() - 1;
        while (low <= high) {
            int mid = (low + high) >>> 1;
            DirectoryNodeReference entry = directory.get(mid);
            if (compareDirectoryValue(entry.minFieldValue(), target, orderedComparator) > 0) {
                high = mid - 1;
            } else if (compareDirectoryValue(entry.maxFieldValue(), target, orderedComparator) < 0) {
                low = mid + 1;
            } else {
                return mid;
            }
        }
        return -1;
    }

    private int lowerBoundSummaryIndex(List<DirectoryLeafReference> directory, FieldValue target) {
        int low = 0;
        int high = directory.size();
        while (low < high) {
            int mid = (low + high) >>> 1;
            if (OrderedRangeIndex.compare(directory.get(mid).maxFieldValue(), target) < 0) {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
        return low;
    }

    private int upperBoundSummaryIndex(List<DirectoryLeafReference> directory, FieldValue target) {
        int low = 0;
        int high = directory.size();
        while (low < high) {
            int mid = (low + high) >>> 1;
            if (OrderedRangeIndex.compare(directory.get(mid).minFieldValue(), target) <= 0) {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
        return low;
    }

    private int lowerBoundNodeIndex(List<DirectoryNodeReference> directory, FieldValue target) {
        int low = 0;
        int high = directory.size();
        while (low < high) {
            int mid = (low + high) >>> 1;
            if (OrderedRangeIndex.compare(directory.get(mid).maxFieldValue(), target) < 0) {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
        return low;
    }

    private int upperBoundNodeIndex(List<DirectoryNodeReference> directory, FieldValue target) {
        int low = 0;
        int high = directory.size();
        while (low < high) {
            int mid = (low + high) >>> 1;
            if (OrderedRangeIndex.compare(directory.get(mid).minFieldValue(), target) <= 0) {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
        return low;
    }

    private int findCandidateNodeIndex(List<DirectoryNodeReference> directory, FieldValue target, boolean orderedComparator) {
        if (directory.isEmpty()) {
            return -1;
        }
        int low = 0;
        int high = directory.size();
        while (low < high) {
            int mid = (low + high) >>> 1;
            if (compareDirectoryValue(directory.get(mid).maxFieldValue(), target, orderedComparator) < 0) {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
        return Math.min(low, directory.size() - 1);
    }

    private int lowerBoundDirectoryIndexForPointMutation(
            List<DirectoryValueEntry> directory,
            FieldValue target,
            boolean orderedComparator
    ) {
        int low = 0;
        int high = directory.size();
        while (low < high) {
            int mid = (low + high) >>> 1;
            if (compareDirectoryValue(directory.get(mid).fieldValue(), target, orderedComparator) < 0) {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
        return low;
    }

    private Set<Object> findExactInLeafPage(int firstPage, FieldValue target, boolean orderedComparator) throws IOException {
        DirectoryPage page = decodeDirectoryPage(pageFile.readPage(firstPage));
        int matchIndex = orderedComparator
                ? findOrderedExactDirectoryIndex(page.entries(), target)
                : findExactDirectoryIndex(page.entries(), target);
        if (matchIndex < 0) {
            return Set.of();
        }
        DirectoryValueEntry valueEntry = page.entries().get(matchIndex);
        return readIdentifierChain(valueEntry.firstPage(), valueEntry.pageCount());
    }

    private Set<Object> findRangeInLeafPage(int firstPage, FieldValue fromInclusive, FieldValue toInclusive) throws IOException {
        DirectoryPage page = decodeDirectoryPage(pageFile.readPage(firstPage));
        Set<Object> identifiers = new LinkedHashSet<>();
        int start = lowerBoundDirectoryIndex(page.entries(), fromInclusive);
        int end = upperBoundDirectoryIndex(page.entries(), toInclusive);
        for (int entryIndex = start; entryIndex < end; entryIndex++) {
            DirectoryValueEntry valueEntry = page.entries().get(entryIndex);
            identifiers.addAll(readIdentifierChain(valueEntry.firstPage(), valueEntry.pageCount()));
        }
        return identifiers;
    }

    private LeafMutationPath locateLeafMutationPath(int pageNo, FieldValue target, boolean orderedComparator) throws IOException {
        return locateLeafMutationPath(pageNo, target, orderedComparator, new ArrayList<>());
    }

    private LeafMutationPath locateLeafMutationPath(
            int pageNo,
            FieldValue target,
            boolean orderedComparator,
            List<AncestorLocation> ancestors
    ) throws IOException {
        DirectoryInternalPage page = decodeDirectoryInternalPage(pageFile.readPage(pageNo));
        int childIndex = findCandidateNodeIndex(page.entries(), target, orderedComparator);
        if (childIndex < 0) {
            return null;
        }
        DirectoryNodeReference child = page.entries().get(childIndex);
        List<AncestorLocation> nextAncestors = new ArrayList<>(ancestors);
        nextAncestors.add(new AncestorLocation(pageNo, page, childIndex));
        if (page.childrenAreLeaves()) {
            DirectoryPage leafPage = decodeDirectoryPage(pageFile.readPage(child.childPage()));
            int exactIndex = orderedComparator
                    ? findOrderedExactDirectoryIndex(leafPage.entries(), target)
                    : findExactDirectoryIndex(leafPage.entries(), target);
            int insertionIndex = exactIndex >= 0
                    ? exactIndex
                    : lowerBoundDirectoryIndexForPointMutation(leafPage.entries(), target, orderedComparator);
            DirectoryValueEntry existingEntry = exactIndex >= 0 ? leafPage.entries().get(exactIndex) : null;
            return new LeafMutationPath(child.childPage(), leafPage, insertionIndex, existingEntry, nextAncestors);
        }
        return locateLeafMutationPath(child.childPage(), target, orderedComparator, nextAncestors);
    }

    private Set<Object> findExactInInternalPage(int pageNo, FieldValue target, boolean orderedComparator) throws IOException {
        DirectoryInternalPage page = decodeDirectoryInternalPage(pageFile.readPage(pageNo));
        int matchIndex = findNodeExactIndex(page.entries(), target, orderedComparator);
        if (matchIndex < 0) {
            return Set.of();
        }
        DirectoryNodeReference child = page.entries().get(matchIndex);
        return page.childrenAreLeaves()
                ? findExactInLeafPage(child.childPage(), target, orderedComparator)
                : findExactInInternalPage(child.childPage(), target, orderedComparator);
    }

    private Set<Object> findRangeInInternalPage(int pageNo, FieldValue fromInclusive, FieldValue toInclusive) throws IOException {
        DirectoryInternalPage page = decodeDirectoryInternalPage(pageFile.readPage(pageNo));
        Set<Object> identifiers = new LinkedHashSet<>();
        int start = lowerBoundNodeIndex(page.entries(), fromInclusive);
        int end = upperBoundNodeIndex(page.entries(), toInclusive);
        for (int entryIndex = start; entryIndex < end; entryIndex++) {
            DirectoryNodeReference child = page.entries().get(entryIndex);
            if (page.childrenAreLeaves()) {
                identifiers.addAll(findRangeInLeafPage(child.childPage(), fromInclusive, toInclusive));
            } else {
                identifiers.addAll(findRangeInInternalPage(child.childPage(), fromInclusive, toInclusive));
            }
        }
        return identifiers;
    }

    private void collectDirectoryEntriesFromInternalPage(int pageNo, List<DirectoryValueEntry> entries) throws IOException {
        DirectoryInternalPage page = decodeDirectoryInternalPage(pageFile.readPage(pageNo));
        for (DirectoryNodeReference child : page.entries()) {
            if (page.childrenAreLeaves()) {
                DirectoryPage leafPage = decodeDirectoryPage(pageFile.readPage(child.childPage()));
                entries.addAll(leafPage.entries());
            } else {
                collectDirectoryEntriesFromInternalPage(child.childPage(), entries);
            }
        }
    }

    private void accumulateTreeMetrics(int pageNo, int depth, TreeMetricsAccumulator accumulator) throws IOException {
        DirectoryInternalPage page = decodeDirectoryInternalPage(pageFile.readPage(pageNo));
        accumulator.recordInternalPage(depth, page.entries().size());
        for (DirectoryNodeReference child : page.entries()) {
            if (page.childrenAreLeaves()) {
                DirectoryPage leafPage = decodeDirectoryPage(pageFile.readPage(child.childPage()));
                accumulator.recordLeafPage(depth + 1, leafPage.entries().size());
            } else {
                accumulateTreeMetrics(child.childPage(), depth + 1, accumulator);
            }
        }
    }

    private void collectLeafEntryCounts(int pageNo, List<Integer> counts) throws IOException {
        DirectoryInternalPage page = decodeDirectoryInternalPage(pageFile.readPage(pageNo));
        for (DirectoryNodeReference child : page.entries()) {
            if (page.childrenAreLeaves()) {
                DirectoryPage leafPage = decodeDirectoryPage(pageFile.readPage(child.childPage()));
                counts.add(leafPage.entries().size());
            } else {
                collectLeafEntryCounts(child.childPage(), counts);
            }
        }
    }

    private void collectInternalEntryCounts(int pageNo, List<Integer> counts) throws IOException {
        DirectoryInternalPage page = decodeDirectoryInternalPage(pageFile.readPage(pageNo));
        counts.add(page.entries().size());
        if (!page.childrenAreLeaves()) {
            for (DirectoryNodeReference child : page.entries()) {
                collectInternalEntryCounts(child.childPage(), counts);
            }
        }
    }

    private int findOrderedExactDirectoryIndex(List<DirectoryValueEntry> directory, FieldValue target) {
        int low = 0;
        int high = directory.size() - 1;
        while (low <= high) {
            int mid = (low + high) >>> 1;
            int compare = OrderedRangeIndex.compare(directory.get(mid).fieldValue(), target);
            if (compare < 0) {
                low = mid + 1;
            } else if (compare > 0) {
                high = mid - 1;
            } else {
                return mid;
            }
        }
        return -1;
    }

    private byte[] encodeMembershipPage(int nextPage, Object identifier) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(buffer);
        out.writeInt(nextPage);
        writeIdentifier(out, identifier);
        out.flush();
        return buffer.toByteArray();
    }

    private MembershipPage decodeMembershipPage(byte[] payload) throws IOException {
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(payload));
        int nextPage = in.readInt();
        Object identifier = readIdentifier(in);
        return new MembershipPage(nextPage, identifier);
    }

    private void rewriteMembershipNextPage(int pageNo, int nextPage) throws IOException {
        MembershipPage current = decodeMembershipPage(pageFile.readPage(pageNo));
        pageFile.writePage(pageNo, encodeMembershipPage(nextPage, current.identifier()));
    }

    private void writeFieldValue(DataOutputStream out, FieldValue value) throws IOException {
        if (value instanceof StringValue stringValue) {
            out.writeByte(FIELD_STRING);
            out.writeUTF(stringValue.value());
            return;
        }
        if (value instanceof LongValue longValue) {
            out.writeByte(FIELD_LONG);
            out.writeLong(longValue.value());
            return;
        }
        if (value instanceof DoubleValue doubleValue) {
            out.writeByte(FIELD_DOUBLE);
            out.writeDouble(doubleValue.value());
            return;
        }
        if (value instanceof BooleanValue booleanValue) {
            out.writeByte(FIELD_BOOLEAN);
            out.writeBoolean(booleanValue.value());
            return;
        }
        if (value instanceof ReferenceValue referenceValue) {
            out.writeByte(FIELD_REFERENCE);
            out.writeLong(referenceValue.recordId().value());
            return;
        }
        throw new IOException("Unsupported exact field snapshot value: " + value);
    }

    private FieldValue readFieldValue(DataInputStream in) throws IOException {
        byte type = in.readByte();
        return switch (type) {
            case FIELD_STRING -> new StringValue(in.readUTF());
            case FIELD_LONG -> new LongValue(in.readLong());
            case FIELD_DOUBLE -> new DoubleValue(in.readDouble());
            case FIELD_BOOLEAN -> new BooleanValue(in.readBoolean());
            case FIELD_REFERENCE -> new ReferenceValue(new RecordId(in.readLong()));
            default -> throw new IOException("Unsupported exact field snapshot value type " + type);
        };
    }

    private void writeIdentifier(DataOutputStream out, Object identifier) throws IOException {
        if (identifier instanceof RecordId recordId) {
            out.writeByte(IDENTIFIER_RECORD_ID);
            out.writeLong(recordId.value());
            return;
        }
        if (identifier instanceof String stringValue) {
            out.writeByte(IDENTIFIER_STRING);
            out.writeUTF(stringValue);
            return;
        }
        throw new IOException("Unsupported exact field snapshot identifier: " + identifier);
    }

    private Object readIdentifier(DataInputStream in) throws IOException {
        byte type = in.readByte();
        return switch (type) {
            case IDENTIFIER_RECORD_ID -> new RecordId(in.readLong());
            case IDENTIFIER_STRING -> in.readUTF();
            default -> throw new IOException("Unsupported exact field snapshot identifier type " + type);
        };
    }

    @Override
    public void close() throws IOException {
        pageFile.close();
    }

    private record Manifest(int version, List<ManifestEntry> entries) {
        private Manifest(List<ManifestEntry> entries) {
            this(VERSION, entries);
        }
    }

    private record ManifestFieldKey(byte kind, String namespace, String fieldName) {
    }

    private record MembershipPage(int nextPage, Object identifier) {
    }

    private record DirectoryPage(int nextPage, FieldValue minFieldValue, FieldValue maxFieldValue, List<DirectoryValueEntry> entries) {
    }

    private record DirectorySummaryPage(int nextPage, FieldValue minFieldValue, FieldValue maxFieldValue, List<DirectoryLeafReference> entries) {
    }

    private record DirectoryValueEntry(FieldValue fieldValue, int firstPage, int pageCount, int identifierCount) {
    }

    private record DirectoryLeafReference(FieldValue minFieldValue, FieldValue maxFieldValue, int firstPage, int pageCount, int entryCount) {
    }

    private record DirectoryInternalPage(boolean childrenAreLeaves, FieldValue minFieldValue, FieldValue maxFieldValue, List<DirectoryNodeReference> entries) {
    }

    private record DirectoryNodeReference(FieldValue minFieldValue, FieldValue maxFieldValue, int childPage, int entryCount) {
    }

    private record AncestorLocation(int pageNo, DirectoryInternalPage page, int childIndex) {
    }

    private record LeafMutationPath(
            int leafPageNo,
            DirectoryPage leafPage,
            int entryIndex,
            DirectoryValueEntry existingEntry,
            List<AncestorLocation> ancestors
    ) {
    }

    private record TreeBuildResult(int rootPage, int internalPageCount) {
    }

    private record InternalPageSplit(DirectoryNodeReference leftReference, DirectoryNodeReference rightReference) {
    }

    private record FieldValueRewrite(
            byte kind,
            String namespace,
            String fieldName,
            Map<FieldValue, Set<Object>> fieldValues
    ) {
    }

    private record RewrittenEntry(
            byte kind,
            String namespace,
            String fieldName,
            List<DirectoryValueEntry> directoryEntries
    ) {
        private static RewrittenEntry rebuiltFromDirectoryEntries(
                byte kind,
                String namespace,
                String fieldName,
                List<DirectoryValueEntry> directoryEntries
        ) {
            return new RewrittenEntry(kind, namespace, fieldName, directoryEntries);
        }
    }

    record DirectoryTreeMetrics(
            int height,
            int internalPageCount,
            int leafPageCount,
            int minInternalEntries,
            int maxInternalEntries,
            int minLeafEntries,
            int maxLeafEntries
    ) {
    }

    private static final class TreeMetricsAccumulator {
        private int height;
        private int internalPageCount;
        private int leafPageCount;
        private int minInternalEntries = Integer.MAX_VALUE;
        private int maxInternalEntries = Integer.MIN_VALUE;
        private int minLeafEntries = Integer.MAX_VALUE;
        private int maxLeafEntries = Integer.MIN_VALUE;

        private void recordInternalPage(int depth, int entryCount) {
            height = Math.max(height, depth);
            internalPageCount += 1;
            minInternalEntries = Math.min(minInternalEntries, entryCount);
            maxInternalEntries = Math.max(maxInternalEntries, entryCount);
        }

        private void recordLeafPage(int depth, int entryCount) {
            height = Math.max(height, depth);
            leafPageCount += 1;
            minLeafEntries = Math.min(minLeafEntries, entryCount);
            maxLeafEntries = Math.max(maxLeafEntries, entryCount);
        }

        private DirectoryTreeMetrics toMetrics() {
            return new DirectoryTreeMetrics(
                    height,
                    internalPageCount,
                    leafPageCount,
                    internalPageCount == 0 ? 0 : minInternalEntries,
                    internalPageCount == 0 ? 0 : maxInternalEntries,
                    leafPageCount == 0 ? 0 : minLeafEntries,
                    leafPageCount == 0 ? 0 : maxLeafEntries
            );
        }
    }

    private record ManifestEntry(byte kind, String namespace, String fieldName, FieldValue fieldValue, int firstPage, int pageCount, int totalLength) {
    }
}
