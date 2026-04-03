package biz.digitalindustry.storage.server.service;

import biz.digitalindustry.storage.query.QueryProviderRegistry;
import biz.digitalindustry.storage.query.cypher.CypherQueryProvider;
import biz.digitalindustry.storage.query.sql.SqlQueryProvider;
import io.micronaut.context.annotation.Factory;
import jakarta.inject.Singleton;

@Factory
public class QueryProviderFactory {
    @Singleton
    CypherQueryProvider cypherQueryProvider(GraphStore graphStore) {
        return new CypherQueryProvider(graphStore);
    }

    @Singleton
    SqlQueryProvider sqlQueryProvider(RelationalStoreService relationalStore) {
        return new SqlQueryProvider(relationalStore);
    }

    @Singleton
    ObjectQueryProvider objectQueryProvider(ObjectStoreService objectStore) {
        return new ObjectQueryProvider(objectStore);
    }

    @Singleton
    QueryProviderRegistry queryProviderRegistry(
            CypherQueryProvider cypherProvider,
            SqlQueryProvider sqlProvider,
            ObjectQueryProvider objectProvider
    ) {
        return new QueryProviderRegistry(java.util.List.of(cypherProvider, sqlProvider, objectProvider));
    }
}
