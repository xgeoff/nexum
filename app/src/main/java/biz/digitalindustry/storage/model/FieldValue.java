package biz.digitalindustry.storage.model;

public sealed interface FieldValue permits
        NullValue,
        StringValue,
        LongValue,
        DoubleValue,
        BooleanValue,
        ReferenceValue,
        ListValue {
}
