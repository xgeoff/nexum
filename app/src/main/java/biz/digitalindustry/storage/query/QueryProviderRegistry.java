package biz.digitalindustry.storage.query;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class QueryProviderRegistry {
    private final Map<String, QueryProvider> providers = new LinkedHashMap<>();

    public QueryProviderRegistry(List<? extends QueryProvider> providers) {
        for (QueryProvider provider : providers) {
            this.providers.put(provider.queryType(), provider);
        }
    }

    public QueryProviderRegistry(Map<String, ? extends QueryProvider> providers) {
        this.providers.putAll(providers);
    }

    public Optional<QueryProvider> getProvider(String queryType) {
        return Optional.ofNullable(providers.get(queryType));
    }
}
