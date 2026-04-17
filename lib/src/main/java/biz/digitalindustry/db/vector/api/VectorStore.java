package biz.digitalindustry.db.vector.api;

import biz.digitalindustry.db.model.FieldValue;
import biz.digitalindustry.db.model.Vector;

import java.util.List;

public interface VectorStore extends AutoCloseable {
    void registerCollection(VectorCollectionDefinition collection);

    VectorCollectionDefinition collection(String name);

    VectorDocument upsert(VectorCollectionDefinition collection, VectorDocument document);

    VectorDocument get(VectorCollectionDefinition collection, String key);

    List<VectorDocument> getAll(VectorCollectionDefinition collection);

    List<VectorDocument> findBy(VectorCollectionDefinition collection, String fieldName, FieldValue value);

    List<VectorDocumentMatch> nearest(VectorCollectionDefinition collection, Vector query, int limit);

    boolean delete(VectorCollectionDefinition collection, String key);

    @Override
    void close();
}
