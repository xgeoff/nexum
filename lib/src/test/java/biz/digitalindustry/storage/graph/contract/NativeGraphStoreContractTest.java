package biz.digitalindustry.storage.graph.contract;

import biz.digitalindustry.storage.graph.api.GraphStore;
import biz.digitalindustry.storage.graph.engine.NativeGraphStore;

public class NativeGraphStoreContractTest extends GraphStoreContractTest {
    @Override
    protected GraphStore createStore(String path) {
        return new NativeGraphStore(path);
    }
}
