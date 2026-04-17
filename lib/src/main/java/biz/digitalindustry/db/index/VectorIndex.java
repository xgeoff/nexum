package biz.digitalindustry.db.index;

import biz.digitalindustry.db.model.Vector;

import java.util.List;

public interface VectorIndex<I> {
    void add(I identifier, Vector vector);

    void remove(I identifier);

    List<VectorMatch<I>> nearest(Vector query, int limit);
}
