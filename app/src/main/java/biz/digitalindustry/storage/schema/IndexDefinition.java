package biz.digitalindustry.storage.schema;

import java.util.List;

public record IndexDefinition(
        String name,
        IndexKind kind,
        List<String> fields
) {
    public IndexDefinition {
        fields = List.copyOf(fields);
    }
}
