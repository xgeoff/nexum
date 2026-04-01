package biz.digitalindustry.graph.contract;

import biz.digitalindustry.graph.api.GraphStore;
import biz.digitalindustry.graph.engine.NativeGraphStore;

public class NativeGraphStoreContractTest extends GraphStoreContractTest {
    @Override
    protected GraphStore createStore(String path) {
        return new NativeGraphStore(path);
    }
}
