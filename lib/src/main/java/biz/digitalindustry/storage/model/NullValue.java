package biz.digitalindustry.storage.model;

public record NullValue() implements FieldValue {
    public static final NullValue INSTANCE = new NullValue();
}
