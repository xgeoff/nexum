package biz.digitalindustry.db.graph.contract;

import biz.digitalindustry.db.graph.api.GraphStore;
import biz.digitalindustry.db.graph.runtime.NativeGraphStore;

public class NativeGraphStoreContractTest extends GraphStoreContractTest {
    @Override
    protected GraphStore createStore(String path) {
        return new NativeGraphStore(path);
    }
}
