package biz.digitalindustry.db.vector;

import biz.digitalindustry.db.model.Vector;

@FunctionalInterface
public interface DistanceFunction {
    float distance(Vector left, Vector right);
}
