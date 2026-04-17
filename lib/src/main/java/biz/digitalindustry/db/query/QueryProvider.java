package biz.digitalindustry.db.query;

public interface QueryProvider {
    String queryType();
    QueryResult execute(QueryCommand command);
}
