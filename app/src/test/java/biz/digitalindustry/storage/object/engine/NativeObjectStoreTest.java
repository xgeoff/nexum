package biz.digitalindustry.storage.object.engine;

import biz.digitalindustry.storage.engine.NativeStorageEngine;
import biz.digitalindustry.storage.object.api.ObjectCodec;
import biz.digitalindustry.storage.object.api.ObjectStoreContext;
import biz.digitalindustry.storage.object.api.ObjectType;
import biz.digitalindustry.storage.object.api.StoredObject;
import biz.digitalindustry.storage.object.api.StoredObjectView;
import biz.digitalindustry.storage.model.FieldValue;
import biz.digitalindustry.storage.model.LongValue;
import biz.digitalindustry.storage.model.Record;
import biz.digitalindustry.storage.model.ReferenceValue;
import biz.digitalindustry.storage.model.StringValue;
import biz.digitalindustry.storage.schema.FieldDefinition;
import biz.digitalindustry.storage.schema.IndexDefinition;
import biz.digitalindustry.storage.schema.IndexKind;
import biz.digitalindustry.storage.schema.ValueType;
import org.junit.After;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class NativeObjectStoreTest {
    private static final ObjectType<Address> ADDRESS_TYPE = new ObjectType<>(
            "AddressObject",
            List.of(
                    new FieldDefinition("city", ValueType.STRING, true, false)
            ),
            List.of(
                    new IndexDefinition("address_city_idx", IndexKind.NON_UNIQUE, List.of("city"))
            ),
            new ObjectCodec<>() {
                @Override
                public String key(Address object) {
                    return object.id();
                }

                @Override
                public Map<String, FieldValue> encode(Address object, ObjectStoreContext context) {
                    return Map.of("city", new StringValue(object.city()));
                }

                @Override
                public Address decode(StoredObjectView view, ObjectStoreContext context) {
                    return new Address(view.key(), stringField(view, "city"));
                }
            }
    );

    private static final ObjectType<Person> PERSON_TYPE = new ObjectType<>(
            "PersonObject",
            List.of(
                    new FieldDefinition("name", ValueType.STRING, true, false),
                    new FieldDefinition("age", ValueType.LONG, true, false),
                    new FieldDefinition("address", ValueType.REFERENCE, false, false)
            ),
            List.of(
                    new IndexDefinition("person_name_idx", IndexKind.NON_UNIQUE, List.of("name")),
                    new IndexDefinition("person_address_idx", IndexKind.REFERENCE, List.of("address"))
            ),
            new ObjectCodec<>() {
                @Override
                public String key(Person object) {
                    return object.id();
                }

                @Override
                public Map<String, FieldValue> encode(Person object, ObjectStoreContext context) {
                    return Map.of(
                            "name", new StringValue(object.name()),
                            "age", new LongValue(object.age()),
                            "address", context.reference(ADDRESS_TYPE, object.address().id())
                    );
                }

                @Override
                public Person decode(StoredObjectView view, ObjectStoreContext context) {
                    return new Person(
                            view.key(),
                            stringField(view, "name"),
                            longField(view, "age"),
                            context.resolve(ADDRESS_TYPE, referenceField(view, "address"))
                    );
                }
            }
    );

    private NativeObjectStore store;
    private Path dbPath;

    @After
    public void tearDown() throws Exception {
        if (store != null) {
            store.close();
            store = null;
        }
        if (dbPath != null) {
            Files.deleteIfExists(Path.of(dbPath + ".records"));
            Files.deleteIfExists(Path.of(dbPath + ".wal"));
            Files.deleteIfExists(dbPath);
        }
    }

    @Test
    public void testSaveGetAndResolveReference() throws Exception {
        openStore("native-object-store-basic");

        StoredObject<Address> address = store.save(ADDRESS_TYPE, new Address("addr-1", "Oakland"));
        StoredObject<Person> person = store.save(PERSON_TYPE, new Person("person-1", "Ada", 37L, address.value()));

        assertNotNull(address.id());
        assertNotNull(person.id());

        StoredObject<Person> loaded = store.get(PERSON_TYPE, "person-1");
        assertNotNull(loaded);
        assertEquals("Ada", loaded.value().name());
        assertEquals(37L, loaded.value().age());
        assertEquals("addr-1", loaded.value().address().id());
        assertEquals("Oakland", loaded.value().address().city());

        List<StoredObject<Person>> allPeople = store.getAll(PERSON_TYPE);
        assertEquals(1, allPeople.size());
        assertEquals("person-1", allPeople.get(0).key());
    }

    @Test
    public void testSaveUpdatesExistingObjectByKey() throws Exception {
        openStore("native-object-store-update");

        store.save(ADDRESS_TYPE, new Address("addr-1", "Oakland"));
        StoredObject<Person> created = store.save(PERSON_TYPE, new Person("person-1", "Ada", 37L, new Address("addr-1", "Oakland")));
        StoredObject<Person> updated = store.save(PERSON_TYPE, new Person("person-1", "Ada Lovelace", 38L, new Address("addr-1", "Oakland")));

        assertEquals(created.id(), updated.id());

        StoredObject<Person> loaded = store.get(PERSON_TYPE, "person-1");
        assertEquals("Ada Lovelace", loaded.value().name());
        assertEquals(38L, loaded.value().age());
    }

    @Test
    public void testDeleteRemovesObjectByKey() throws Exception {
        openStore("native-object-store-delete");

        store.save(ADDRESS_TYPE, new Address("addr-1", "Oakland"));
        store.save(PERSON_TYPE, new Person("person-1", "Ada", 37L, new Address("addr-1", "Oakland")));

        assertTrue(store.delete(PERSON_TYPE, "person-1"));
        assertNull(store.get(PERSON_TYPE, "person-1"));
        assertFalse(store.delete(PERSON_TYPE, "person-1"));
    }

    @Test
    public void testFindByExactFieldValue() throws Exception {
        openStore("native-object-store-find");

        store.save(ADDRESS_TYPE, new Address("addr-1", "Oakland"));
        store.save(ADDRESS_TYPE, new Address("addr-2", "Berkeley"));
        store.save(PERSON_TYPE, new Person("person-1", "Ada", 37L, new Address("addr-1", "Oakland")));
        store.save(PERSON_TYPE, new Person("person-2", "Grace", 37L, new Address("addr-2", "Berkeley")));
        store.save(PERSON_TYPE, new Person("person-3", "Linus", 45L, new Address("addr-1", "Oakland")));

        List<StoredObject<Person>> ageMatches = store.findBy(PERSON_TYPE, "age", new LongValue(37L));
        assertEquals(2, ageMatches.size());
        assertEquals("person-1", ageMatches.get(0).key());
        assertEquals("person-2", ageMatches.get(1).key());

        StoredObject<Person> nameMatch = store.findOneBy(PERSON_TYPE, "name", new StringValue("Linus"));
        assertNotNull(nameMatch);
        assertEquals("person-3", nameMatch.key());
    }

    @Test
    public void testFindByReferenceField() throws Exception {
        openStore("native-object-store-reference-find");

        StoredObject<Address> address = store.save(ADDRESS_TYPE, new Address("addr-1", "Oakland"));
        store.save(PERSON_TYPE, new Person("person-1", "Ada", 37L, address.value()));
        store.save(PERSON_TYPE, new Person("person-2", "Linus", 45L, address.value()));

        List<StoredObject<Person>> matches = store.findBy(PERSON_TYPE, "address", new ReferenceValue(address.id()));
        assertEquals(2, matches.size());
        assertEquals("person-1", matches.get(0).key());
        assertEquals("person-2", matches.get(1).key());
    }

    @Test
    public void testFindByReflectsUpdatesDeletesAndReopen() throws Exception {
        openStore("native-object-store-index-rebuild");

        store.save(ADDRESS_TYPE, new Address("addr-1", "Oakland"));
        store.save(PERSON_TYPE, new Person("person-1", "Ada", 37L, new Address("addr-1", "Oakland")));
        store.save(PERSON_TYPE, new Person("person-2", "Grace", 37L, new Address("addr-1", "Oakland")));

        store.save(PERSON_TYPE, new Person("person-2", "Grace Hopper", 38L, new Address("addr-1", "Oakland")));
        store.delete(PERSON_TYPE, "person-1");

        List<StoredObject<Person>> ageMatches = store.findBy(PERSON_TYPE, "age", new LongValue(37L));
        assertEquals(0, ageMatches.size());

        List<StoredObject<Person>> updatedMatches = store.findBy(PERSON_TYPE, "name", new StringValue("Grace Hopper"));
        assertEquals(1, updatedMatches.size());
        assertEquals("person-2", updatedMatches.get(0).key());

        store.close();
        store = null;

        store = new NativeObjectStore(dbPath.toString());
        store.registerType(ADDRESS_TYPE);
        store.registerType(PERSON_TYPE);

        List<StoredObject<Person>> reopenedMatches = store.findBy(PERSON_TYPE, "name", new StringValue("Grace Hopper"));
        assertEquals(1, reopenedMatches.size());
        assertEquals("person-2", reopenedMatches.get(0).key());
    }

    @Test
    public void testReopenPreservesObjectsAndReferences() throws Exception {
        openStore("native-object-store-reopen");

        store.save(ADDRESS_TYPE, new Address("addr-1", "Oakland"));
        store.save(PERSON_TYPE, new Person("person-1", "Ada", 37L, new Address("addr-1", "Oakland")));
        store.close();
        store = null;

        store = new NativeObjectStore(dbPath.toString());
        store.registerType(ADDRESS_TYPE);
        store.registerType(PERSON_TYPE);

        StoredObject<Person> reopened = store.get(PERSON_TYPE, "person-1");
        assertNotNull(reopened);
        assertEquals("Ada", reopened.value().name());
        assertEquals("Oakland", reopened.value().address().city());
    }

    @Test
    public void testReaderDuringWriteSeesLastCommittedObjectState() throws Exception {
        openStore("native-object-overlap");

        store.save(ADDRESS_TYPE, new Address("addr-1", "Oakland"));
        store.save(PERSON_TYPE, new Person("person-1", "Ada", 37L, new Address("addr-1", "Oakland")));
        NativeStorageEngine engine = underlyingEngine(store);

        CountDownLatch readerDone = new CountDownLatch(1);
        AtomicReference<StoredObject<Person>> observed = new AtomicReference<>();

        try (var tx = engine.begin(biz.digitalindustry.storage.tx.TransactionMode.READ_WRITE)) {
            var recordId = (biz.digitalindustry.storage.model.RecordId) engine
                    .exactIndexFind("object:PersonObject", ObjectType.KEY_FIELD, new StringValue("person-1"))
                    .iterator()
                    .next();
            Record current = engine.recordStore().get(recordId);
            engine.recordStore().update(new Record(
                    current.id(),
                    current.type(),
                    Map.of(
                            ObjectType.KEY_FIELD, new StringValue("person-1"),
                            "name", new StringValue("Ada Lovelace"),
                            "age", new LongValue(37L),
                            "address", current.fields().get("address")
                    )
            ));

            Thread reader = new Thread(() -> {
                try {
                    observed.set(store.get(PERSON_TYPE, "person-1"));
                } finally {
                    readerDone.countDown();
                }
            });
            reader.start();

            assertTrue(readerDone.await(2, TimeUnit.SECONDS));
            assertEquals("Ada", observed.get().value().name());
            tx.commit();
        }

        assertEquals("Ada Lovelace", store.get(PERSON_TYPE, "person-1").value().name());
    }

    private void openStore(String prefix) throws Exception {
        dbPath = Files.createTempFile(prefix, ".dbs");
        Files.deleteIfExists(dbPath);
        store = new NativeObjectStore(dbPath.toString());
        store.registerType(ADDRESS_TYPE);
        store.registerType(PERSON_TYPE);
    }

    private static String stringField(StoredObjectView view, String field) {
        FieldValue value = view.fields().get(field);
        return value instanceof StringValue stringValue ? stringValue.value() : null;
    }

    private static long longField(StoredObjectView view, String field) {
        FieldValue value = view.fields().get(field);
        return value instanceof LongValue longValue ? longValue.value() : 0L;
    }

    private static ReferenceValue referenceField(StoredObjectView view, String field) {
        FieldValue value = view.fields().get(field);
        return value instanceof ReferenceValue referenceValue ? referenceValue : null;
    }

    private NativeStorageEngine underlyingEngine(NativeObjectStore store) throws Exception {
        var field = NativeObjectStore.class.getDeclaredField("storageEngine");
        field.setAccessible(true);
        return (NativeStorageEngine) field.get(store);
    }

    private record Address(String id, String city) {
    }

    private record Person(String id, String name, long age, Address address) {
    }
}
