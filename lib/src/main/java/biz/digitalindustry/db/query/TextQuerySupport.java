package biz.digitalindustry.db.query;

public final class TextQuerySupport {
    private TextQuerySupport() {
    }

    public static String requireQueryText(QueryCommand command) {
        Object queryText = command.payload().get("queryText");
        if (!(queryText instanceof String text) || text.isBlank()) {
            throw new IllegalArgumentException("queryText must not be null or empty");
        }
        return text;
    }
}
