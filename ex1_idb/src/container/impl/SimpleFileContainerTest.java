package container.impl;

import io.FixedSizeSerializer;
import io.IntSerializer;
import io.LongSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import util.MetaData;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import static org.junit.jupiter.api.Assertions.*;

class SimpleFileContainerTest {

    private SimpleFileContainer<String> container;
    private SimpleFileContainer<Integer> intContainer;
    private SimpleFileContainer<Long> longContainer;
    private Path tempDirectory;

    // Serializer for fixed-size string serialization (for testing purposes)
    private record FixedSizeStringSerializer(int size) implements FixedSizeSerializer<String> {

        @Override
        public int getSerializedSize() {
            return size;
        }

        @Override
        public void serialize(String value, ByteBuffer buffer) {
            byte[] data = value.getBytes();
            buffer.put(data, 0, Math.min(data.length, size));
            for (int i = data.length; i < size; i++) {
                buffer.put((byte) ' '); // Padding for fixed-size
            }
        }

        @Override
        public String deserialize(ByteBuffer buffer) {
            byte[] data = new byte[size];
            buffer.get(data);
            return new String(data).trim();
        }
    }

    @BeforeEach
    void setUp() throws Exception {
        tempDirectory = Files.createTempDirectory("SimpleFileContainerTest");
        container = new SimpleFileContainer<>(tempDirectory, "testContainer", new FixedSizeStringSerializer(10));
        intContainer = new SimpleFileContainer<>(tempDirectory, "intContainer", new IntSerializer());
        longContainer = new SimpleFileContainer<>(tempDirectory, "longContainer", new LongSerializer());
        container.open();
        intContainer.open();
        longContainer.open();
    }

    @AfterEach
    void tearDown() {
        container.close();
        intContainer.close();
        longContainer.close();
        tempDirectory.toFile().deleteOnExit();
    }

    @Test
    void testReserve() {
        Long key1 = container.reserve();
        Long key2 = container.reserve();
        assertNotNull(key1);
        assertNotNull(key2);
        assertNotEquals(key1, key2);
        assertEquals(0, key1); // First key should be 0
        assertEquals(1, key2); // Second key should be 1
    }

    @Test
    void testReserveIntContainer() {
        Long key1 = intContainer.reserve();
        Long key2 = intContainer.reserve();
        assertNotNull(key1);
        assertNotNull(key2);
        assertNotEquals(key1, key2);
        assertEquals(0, key1);
        assertEquals(1, key2);
    }

    @Test
    void testReserveLongContainer() {
        Long key1 = longContainer.reserve();
        Long key2 = longContainer.reserve();
        assertNotNull(key1);
        assertNotNull(key2);
        assertNotEquals(key1, key2);
        assertEquals(0, key1);
        assertEquals(1, key2);
    }

    @Test
    void testSequentialReserveStringContainer() {
        List<Long> keys = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            keys.add(container.reserve());
        }

        // Verify keys are sequential
        for (int i = 0; i < keys.size(); i++) {
            assertEquals(i, keys.get(i));
        }
    }

    @Test
    void testSequentialReserveIntContainer() {
        List<Long> keys = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            keys.add(intContainer.reserve());
        }

        for (int i = 0; i < keys.size(); i++) {
            assertEquals(i, keys.get(i));
        }
    }

    @Test
    void testSequentialReserveLongContainer() {
        List<Long> keys = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            keys.add(longContainer.reserve());
        }

        for (int i = 0; i < keys.size(); i++) {
            assertEquals(i, keys.get(i));
        }
    }

    @Test
    void testUpdateAndGetStringContainer() {
        Long key = container.reserve();
        String value = "hello";
        container.update(key, value);

        String retrievedValue = container.get(key);
        assertEquals(value, retrievedValue);
    }

    @Test
    void testUpdateAndGetIntContainer() {
        Long key = intContainer.reserve();
        intContainer.update(key, 1);
        assertEquals(1, intContainer.get(key));
    }

    @Test
    void testUpdateAndGetLongContainer() {
        Long key = longContainer.reserve();
        longContainer.update(key, 1L);
        assertEquals(1L, longContainer.get(key));
    }

    @Test
    void testMultipleUpdatesOnSameKeyStringContainer() {
        Long key = container.reserve();

        container.update(key, "first");
        assertEquals("first", container.get(key));

        container.update(key, "second");
        assertEquals("second", container.get(key));

        container.update(key, "third");
        assertEquals("third", container.get(key));
    }

    @Test
    void testMultipleUpdatesOnSameKeyIntContainer() {
        Long key = intContainer.reserve();

        intContainer.update(key, 1);
        assertEquals(1, intContainer.get(key));

        intContainer.update(key, 2);
        assertEquals(2, intContainer.get(key));

        intContainer.update(key, 3);
        assertEquals(3, intContainer.get(key));
    }

    @Test
    void testMultipleUpdatesOnSameKeyLongContainer() {
        Long key = longContainer.reserve();

        longContainer.update(key, 1L);
        assertEquals(1L, longContainer.get(key));

        longContainer.update(key, 2L);
        assertEquals(2L, longContainer.get(key));

        longContainer.update(key, 3L);
        assertEquals(3L, longContainer.get(key));
    }

    @Test
    void testRemoveAndGetThrowsExceptionIntContainer() {
        Long key = intContainer.reserve();
        intContainer.update(key, 1);
        intContainer.remove(key);
        assertThrows(NoSuchElementException.class, () -> intContainer.remove(key));
        assertThrows(NoSuchElementException.class, () -> intContainer.get(key));
    }

    @Test
    void testRemoveAndGetThrowsExceptionLongContainer() {
        Long key = longContainer.reserve();
        longContainer.update(key, 1L);
        longContainer.remove(key);
        assertThrows(NoSuchElementException.class, () -> longContainer.remove(key));
        assertThrows(NoSuchElementException.class, () -> longContainer.get(key));
    }

    @Test
    void testUpdateWithMaxLengthString() {
        Long key = container.reserve();
        String value = "1234567890"; // Exactly 10 characters
        container.update(key, value);
        assertEquals(value, container.get(key));
    }

    @Test
    void testUpdateWithTooLongString() {
        Long key = container.reserve();
        String value = "12345678901234567890"; // 20 characters
        container.update(key, value);
        assertEquals("1234567890", container.get(key)); // Should only store first 10 characters
    }

    @Test
    void testUpdateNonexistentKeyThrowsException() {
        Long nonexistentKey = 999L;
        assertThrows(NoSuchElementException.class, () -> container.update(nonexistentKey, "test"));
    }

    @Test
    void testGetNonexistentKeyThrowsException() {
        Long nonexistentKey = 999L;
        assertThrows(NoSuchElementException.class, () -> container.get(nonexistentKey));
    }

    @Test
    void testRemoveAndGetThrowsException() {
        Long key = container.reserve();
        container.update(key, "helloWorld");
        container.remove(key);
        assertThrows(NoSuchElementException.class, () -> container.get(key));
    }

    @Test
    void testRemoveNonexistentKeyThrowsException() {
        Long nonexistentKey = 999L;
        assertThrows(NoSuchElementException.class, () -> container.remove(nonexistentKey));
    }

    @Test
    void testMetaDataAfterOpenStringContainer() {
        container.close();
        container.open();
        MetaData metaData = container.getMetaData();
        assertEquals(0, metaData.getLongProperty("recentKey", -1));
        assertEquals(11, metaData.getIntProperty("objectSize", -1)); // 10 + 1 for deletion marker
    }

    @Test
    void testMetaDataAfterOpenIntContainer() {
        intContainer.close();
        intContainer.open();
        MetaData metaData = intContainer.getMetaData();
        assertEquals(0, metaData.getLongProperty("recentKey", -1));
        assertEquals(5, metaData.getIntProperty("objectSize", -1)); // 4 + 1 for deletion marker
    }

    @Test
    void testMetaDataAfterOpenLongContainer() {
        longContainer.close();
        longContainer.open();
        MetaData metaData = longContainer.getMetaData();
        assertEquals(0, metaData.getLongProperty("recentKey", -1));
        assertEquals(9, metaData.getIntProperty("objectSize", -1)); // 8 + 1 for deletion marker
    }

    @Test
    void testPersistenceAcrossReopeningStringContainer() {
        // Store some data
        Long key1 = container.reserve();
        Long key2 = container.reserve();
        container.update(key1, "first");
        container.update(key2, "second");

        // Close and reopen
        container.close();
        container.open();

        // Verify data persists
        assertEquals("first", container.get(key1));
        assertEquals("second", container.get(key2));
        assertEquals(2, container.getMetaData().getLongProperty("recentKey", -1));
    }

    @Test
    void testPersistenceAcrossReopeningIntContainer() {
        Long key = intContainer.reserve();
        intContainer.update(key, 1);
        intContainer.close();
        intContainer.open();
        assertEquals(1, intContainer.get(key));
    }

    @Test
    void testPersistenceAcrossReopeningLongContainer() {
        Long key = longContainer.reserve();
        longContainer.update(key, 1L);
        longContainer.close();
        longContainer.open();
        assertEquals(1L, longContainer.get(key));
    }

    @Test
    void testRemoveAndReserveNewKeyStringContainer() {
        Long key1 = container.reserve();
        container.update(key1, "first");
        container.remove(key1);

        Long key2 = container.reserve();
        assertNotEquals(key1, key2); // Verify keys are not reused

        container.update(key2, "second");
        assertThrows(NoSuchElementException.class, () -> container.get(key1));
        assertEquals("second", container.get(key2));
    }

    @Test
    void testRemoveAndReserveNewKeyIntContainer() {
        Long key1 = intContainer.reserve();
        intContainer.update(key1, 1);
        intContainer.remove(key1);

        Long key2 = intContainer.reserve();
        assertNotEquals(key1, key2);

        intContainer.update(key2, 2);
        assertThrows(NoSuchElementException.class, () -> intContainer.get(key1));
        assertEquals(2, intContainer.get(key2));
    }

    @Test
    void testRemoveAndReserveNewKeyLongContainer() {
        Long key1 = longContainer.reserve();
        longContainer.update(key1, 1L);
        longContainer.remove(key1);

        Long key2 = longContainer.reserve();
        assertNotEquals(key1, key2);

        longContainer.update(key2, 2L);
        assertThrows(NoSuchElementException.class, () -> longContainer.get(key1));
        assertEquals(2L, longContainer.get(key2));
    }

    @Test
    void testNullInputs() {
        Long key = container.reserve();
        Long intKey = intContainer.reserve();
        Long longKey = longContainer.reserve();
        assertThrows(IllegalArgumentException.class, () -> container.update(key, null));
        assertThrows(IllegalArgumentException.class, () -> container.update(null, "test"));
        assertThrows(IllegalArgumentException.class, () -> container.get(null));
        assertThrows(IllegalArgumentException.class, () -> container.remove(null));

        assertThrows(IllegalArgumentException.class, () -> intContainer.update(intKey, null));
        assertThrows(IllegalArgumentException.class, () -> intContainer.update(null, 1));
        assertThrows(IllegalArgumentException.class, () -> intContainer.get(null));
        assertThrows(IllegalArgumentException.class, () -> intContainer.remove(null));

        assertThrows(IllegalArgumentException.class, () -> longContainer.update(longKey, null));
        assertThrows(IllegalArgumentException.class, () -> longContainer.update(null, 1L));
        assertThrows(IllegalArgumentException.class, () -> longContainer.get(null));
        assertThrows(IllegalArgumentException.class, () -> longContainer.remove(null));
    }

    @Test
    void testEmptyString() {
        Long key = container.reserve();
        container.update(key, "");
        assertEquals("", container.get(key));
    }

    @Test
    void testMultipleRemoves() {
        Long key = container.reserve();
        container.update(key, "test");
        container.remove(key);
        assertThrows(NoSuchElementException.class, () -> container.remove(key));
        assertThrows(NoSuchElementException.class, () -> container.get(key));
    }
}