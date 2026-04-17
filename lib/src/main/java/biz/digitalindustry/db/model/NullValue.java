package biz.digitalindustry.db.model;

public record NullValue() implements FieldValue {
    public static final NullValue INSTANCE = new NullValue();
}
