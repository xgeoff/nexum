package biz.digitalindustry.storage.query;

public interface QueryProvider {
    String queryType();
    QueryResult execute(QueryCommand command);
}
