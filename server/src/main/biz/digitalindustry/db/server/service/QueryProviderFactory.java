package biz.digitalindustry.db.server.service;

import biz.digitalindustry.db.query.QueryProviderRegistry;
import biz.digitalindustry.db.query.cypher.CypherQueryProvider;
import biz.digitalindustry.db.query.sql.SqlQueryProvider;
import biz.digitalindustry.db.query.vector.VectorQueryProvider;
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
    VectorQueryProvider vectorQueryProvider(VectorStoreService vectorStore) {
        return new VectorQueryProvider(vectorStore);
    }

    @Singleton
    QueryProviderRegistry queryProviderRegistry(
            CypherQueryProvider cypherProvider,
            SqlQueryProvider sqlProvider,
            ObjectQueryProvider objectProvider,
            VectorQueryProvider vectorProvider
    ) {
        return new QueryProviderRegistry(java.util.List.of(cypherProvider, sqlProvider, objectProvider, vectorProvider));
    }
}
