package biz.digitalindustry.db.index;

import biz.digitalindustry.db.engine.VectorIndexManager;
import biz.digitalindustry.db.model.Vector;
import biz.digitalindustry.db.vector.Distances;
import biz.digitalindustry.db.vector.FlatVectorIndex;
import biz.digitalindustry.db.vector.VectorIndexDefinition;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class VectorIndexTest {
    @Test
    public void testFlatIndexReturnsNearestNeighborsInDistanceOrder() {
        FlatVectorIndex<String> index = new FlatVectorIndex<>(3, Distances::euclidean);
        index.add("a", Vector.of(1.0f, 0.0f, 0.0f));
        index.add("b", Vector.of(0.9f, 0.1f, 0.0f));
        index.add("c", Vector.of(0.0f, 1.0f, 0.0f));

        List<VectorMatch<String>> matches = index.nearest(Vector.of(1.0f, 0.0f, 0.0f), 2);

        assertEquals(2, matches.size());
        assertEquals("a", matches.get(0).id());
        assertEquals("b", matches.get(1).id());
    }

    @Test
    public void testManagerKeepsVectorNamespacesIsolated() {
        VectorIndexManager manager = new VectorIndexManager();
        manager.ensureNamespace("table:items", List.of(new VectorIndexDefinition("embedding", 3, Distances.EUCLIDEAN)));
        manager.ensureNamespace("table:documents", List.of(new VectorIndexDefinition("embedding", 3, Distances.EUCLIDEAN)));

        manager.add("table:items", "embedding", Vector.of(1.0f, 0.0f, 0.0f), "i1");
        manager.add("table:documents", "embedding", Vector.of(0.0f, 1.0f, 0.0f), "d1");

        assertEquals("i1", manager.nearest("table:items", "embedding", Vector.of(1.0f, 0.0f, 0.0f), 1).get(0).id());
        assertEquals("d1", manager.nearest("table:documents", "embedding", Vector.of(0.0f, 1.0f, 0.0f), 1).get(0).id());
    }
}
