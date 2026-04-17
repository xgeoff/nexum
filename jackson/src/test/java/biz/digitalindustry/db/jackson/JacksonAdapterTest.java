package biz.digitalindustry.db.jackson;

import biz.digitalindustry.db.object.api.ObjectTypeDefinition;
import biz.digitalindustry.db.object.api.StoredObject;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class JacksonAdapterTest {
    @Test
    public void testRegisterSaveAndGetRecordDto() {
        try (var adapter = JacksonAdapter.memoryOnly()) {
            ObjectTypeDefinition definition = adapter.register(PersonDto.class, "id");
            assertEquals("PersonDto", definition.name());
            assertEquals("id", definition.keyField());

            StoredObject<PersonDto> saved = adapter.save(new PersonDto("person-1", "Ada", 37L, true));
            assertNotNull(saved);
            assertEquals("person-1", saved.key());

            StoredObject<PersonDto> loaded = adapter.get(PersonDto.class, "person-1");
            assertNotNull(loaded);
            assertEquals("Ada", loaded.value().name());
            assertEquals(37L, loaded.value().age());
            assertTrue(loaded.value().active());
        }
    }

    @Test
    public void testRegisterWithCustomizerCanAddIndex() {
        try (var adapter = JacksonAdapter.memoryOnly()) {
            ObjectTypeDefinition definition = adapter.register(PersonDto.class, "id",
                    builder -> builder.index("person_name_idx").on("name"));

            assertEquals(1, definition.indexes().size());
            assertEquals("person_name_idx", definition.indexes().get(0).name());
            assertEquals("name", definition.indexes().get(0).fields().get(0));
        }
    }

    @Test
    public void testDeleteRegisteredDto() {
        try (var adapter = JacksonAdapter.memoryOnly()) {
            adapter.register(PersonDto.class, "id");
            adapter.save(new PersonDto("person-1", "Ada", 37L, true));

            assertTrue(adapter.delete(PersonDto.class, "person-1"));
            assertNull(adapter.get(PersonDto.class, "person-1"));
        }
    }

    public record PersonDto(
            String id,
            String name,
            long age,
            boolean active
    ) {
    }
}
