package biz.digitalindustry.db.server.queryhandler;

import biz.digitalindustry.db.server.model.QueryResponse;

public interface QueryHandler {
    QueryResponse handle(String query);
}
