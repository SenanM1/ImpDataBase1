package container.impl;


import org.junit.jupiter.api.Test;

import java.util.NoSuchElementException;

import static org.junit.jupiter.api.Assertions.*;

public class MapContainerTest {

    /**
     * Tests whether the getMetaData method returns correct value.
     */
    @Test
    void getMetaDataTest() {
        MapContainer<Integer> mapContainer = new MapContainer<>();
        assertEquals(mapContainer.getMetaData().getIntProperty("size"), 0);

        mapContainer.reserve();
        mapContainer.update(0L, 1);

        assertEquals(mapContainer.getMetaData().getIntProperty("size"), 1);
    }

    /**
     * Tests whether the reserve method creates a new key and saves it.
     */
    @Test
    void reserveTest() {
        MapContainer<Integer> mapContainer = new MapContainer<>();
        assertEquals(mapContainer.reserve(), 0L);
        assertEquals(mapContainer.reserve(), 1L);
        assertEquals(mapContainer.reserve(), 2L);
    }

    /**
     * Tests whether the get method return a corresponding value to the key.
     */
    @Test
    void getTest() {
        MapContainer<Integer> mapContainer = new MapContainer<>();
        mapContainer.reserve();
        mapContainer.reserve();
        mapContainer.reserve();

        assertNull(mapContainer.get(0L));
        assertNull(mapContainer.get(1L));
        assertNull(mapContainer.get(2L));

        mapContainer.update(0L, 1);

        assertEquals(mapContainer.get(0L), 1);
    }

    /**
     * Tests whether the update method actually updates the value for the given key in the map.
     */
    @Test
    void updateTest() {
        MapContainer<Integer> mapContainer = new MapContainer<>();
        mapContainer.reserve();
        mapContainer.reserve();
        mapContainer.reserve();

        mapContainer.update(0L, 1);
        mapContainer.update(1L, 2);
        mapContainer.update(2L, 3);

        assertEquals(mapContainer.get(0L), 1);
        assertEquals(mapContainer.get(1L), 2);
        assertEquals(mapContainer.get(2L), 3);
    }

    /**
     * Tests whether the remove method removes the specified key from the map.
     */
    @Test
    void removeTest() {
        MapContainer<Integer> mapContainer = new MapContainer<>();
        mapContainer.reserve();
        mapContainer.reserve();
        mapContainer.reserve();

        mapContainer.update(0L, 1);
        mapContainer.update(1L, 2);
        mapContainer.update(2L, 3);

        mapContainer.remove(0L);
        mapContainer.remove(1L);
        mapContainer.remove(2L);

        assertThrows(NoSuchElementException.class, () -> mapContainer.remove(0L));
        assertThrows(NoSuchElementException.class, () -> mapContainer.remove(1L));
        assertThrows(NoSuchElementException.class, () -> mapContainer.remove(2L));
    }
}
