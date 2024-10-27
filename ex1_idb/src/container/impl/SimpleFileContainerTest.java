package container.impl;

import io.FixedSizeSerializer;
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
        container.open();
    }

    @AfterEach
    void tearDown() {
        container.close();
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
    void testSequentialReserve() {
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
    void testUpdateAndGet() {
        Long key = container.reserve();
        String value = "hello";
        container.update(key, value);

        String retrievedValue = container.get(key);
        assertEquals(value, retrievedValue);
    }

    @Test
    void testMultipleUpdatesOnSameKey() {
        Long key = container.reserve();

        container.update(key, "first");
        assertEquals("first", container.get(key));

        container.update(key, "second");
        assertEquals("second", container.get(key));

        container.update(key, "third");
        assertEquals("third", container.get(key));
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
    void testMetaDataAfterOpen() {
        container.close();
        container.open();
        MetaData metaData = container.getMetaData();
        assertEquals(0, metaData.getLongProperty("recentKey", -1));
        assertEquals(11, metaData.getIntProperty("objectSize", -1)); // 10 + 1 for deletion marker
    }

    @Test
    void testPersistenceAcrossReopening() {
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
    void testRemoveAndReserveNewKey() {
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
    void testNullInputs() {
        Long key = container.reserve();
        assertThrows(IllegalArgumentException.class, () -> container.update(key, null));
        assertThrows(IllegalArgumentException.class, () -> container.update(null, "test"));
        assertThrows(IllegalArgumentException.class, () -> container.get(null));
        assertThrows(IllegalArgumentException.class, () -> container.remove(null));
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