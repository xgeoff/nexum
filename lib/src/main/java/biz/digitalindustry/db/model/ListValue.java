package biz.digitalindustry.db.model;

import java.util.List;

public record ListValue(List<FieldValue> values) implements FieldValue {
    public ListValue {
        values = List.copyOf(values);
    }
}
