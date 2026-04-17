package biz.digitalindustry.db.schema;

import java.util.List;

public record EntityType(
        String name,
        List<FieldDefinition> fields,
        List<IndexDefinition> indexes
) {
    public EntityType {
        fields = List.copyOf(fields);
        indexes = List.copyOf(indexes);
    }
}
