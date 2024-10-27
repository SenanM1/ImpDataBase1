package container.impl;

import container.Container;
import io.FixedSizeSerializer;
import util.MetaData;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.NoSuchElementException;

public class SimpleFileContainer<Value> implements Container<Long, Value> {

    private static final byte ACTIVE = 1;
    private static final byte DELETED = 0;

    private final Path dataFilePath;
    private final Path metaFilePath;
    private final FixedSizeSerializer<Value> serializer;

    private long recentKey = 0;
    private boolean isOpen = false;
    private RandomAccessFile dataFile;
    private final MetaData metaData;
    private final int objectSize;

    public SimpleFileContainer(Path directory, String filenamePrefix, FixedSizeSerializer<Value> serializer) {
        if (serializer == null) {
            throw new IllegalArgumentException("Serializer cannot be null");
        }

        this.dataFilePath = directory.resolve(filenamePrefix + "_data.dat");
        this.metaFilePath = directory.resolve(filenamePrefix + "_meta.dat");
        this.serializer = serializer;
        this.objectSize = serializer.getSerializedSize() + 1; // +1 for deletion marker byte
        this.metaData = new MetaData();
    }

    @Override
    public MetaData getMetaData() {
        return metaData;
    }

    @Override
    public void open() throws IllegalStateException {
        if (isOpen) {
            throw new IllegalStateException("Container is already open");
        }

        try {
            Files.createDirectories(dataFilePath.getParent());
            dataFile = new RandomAccessFile(dataFilePath.toFile(), "rw");

            if (Files.exists(metaFilePath)) {
                metaData.readFrom(metaFilePath);
                recentKey = metaData.getLongProperty("recentKey", 0);

                int storedObjectSize = metaData.getIntProperty("objectSize", -1);
                if (storedObjectSize != objectSize) {
                    throw new IllegalStateException("Stored object size does not match current serializer size");
                }
            } else {
                metaData.setLongProperty("recentKey", recentKey);
                metaData.setIntProperty("objectSize", objectSize);
                metaData.writeTo(metaFilePath);
            }
            isOpen = true;
        } catch (IOException e) {
            throw new IllegalStateException("Failed to open container: " + e.getMessage(), e);
        }
    }

    @Override
    public void close() throws IllegalStateException {
        if (!isOpen) {
            throw new IllegalStateException("Container is not open");
        }

        try {
            metaData.setLongProperty("recentKey", recentKey);
            metaData.writeTo(metaFilePath);
            dataFile.close();
            isOpen = false;
        } catch (IOException e) {
            throw new IllegalStateException("Failed to close container: " + e.getMessage(), e);
        }
    }

    @Override
    public Long reserve() throws IllegalStateException {
        checkOpen();

        long key = recentKey++;
        long position = key * objectSize;

        try {
            if (dataFile.length() < position + objectSize) {
                dataFile.setLength(position + objectSize);
            }

            dataFile.seek(position);
            dataFile.writeByte(ACTIVE);
            dataFile.write(new byte[objectSize - 1]);

            return key;
        } catch (IOException e) {
            throw new IllegalStateException("Failed to reserve key " + key + ": " + e.getMessage(), e);
        }
    }

    @Override
    public void update(Long key, Value value) throws IllegalArgumentException, IllegalStateException, NoSuchElementException {
        checkOpen();
        validateKey(key);
        if (value == null) {
            throw new IllegalArgumentException("Value cannot be null");
        }

        long position = key * objectSize;
        try {
            if (position >= dataFile.length()) {
                throw new NoSuchElementException("Key " + key + " does not exist");
            }

            // First check if the entry is deleted
            dataFile.seek(position);
            if (dataFile.read() == DELETED) {
                throw new NoSuchElementException("Key " + key + " has been deleted");
            }

            // Create a buffer for just the serialized data (without the deletion marker)
            ByteBuffer valueBuffer = ByteBuffer.allocate(serializer.getSerializedSize());
            serializer.serialize(value, valueBuffer);
            valueBuffer.flip();

            // Write the deletion marker and then the serialized data
            dataFile.seek(position);
            dataFile.writeByte(ACTIVE);
            dataFile.write(valueBuffer.array());
        } catch (IOException e) {
            throw new IllegalStateException("Failed to update value for key " + key + ": " + e.getMessage(), e);
        }
    }

    @Override
    public Value get(Long key) throws IllegalStateException, NoSuchElementException {
        checkOpen();
        validateKey(key);

        long position = key * objectSize;
        try {
            if (position >= dataFile.length()) {
                throw new NoSuchElementException("Key " + key + " does not exist");
            }

            // Read the deletion marker first
            dataFile.seek(position);
            byte status = dataFile.readByte();

            if (status == DELETED) {
                throw new NoSuchElementException("Key " + key + " has been deleted");
            }

            // Read just the serialized data (without the deletion marker)
            byte[] valueBytes = new byte[serializer.getSerializedSize()];
            int bytesRead = dataFile.read(valueBytes);

            if (bytesRead != serializer.getSerializedSize()) {
                throw new IllegalStateException("Failed to read complete record for key " + key);
            }

            // Create a buffer with just the serialized data
            ByteBuffer valueBuffer = ByteBuffer.wrap(valueBytes);
            return serializer.deserialize(valueBuffer);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to read value for key " + key + ": " + e.getMessage(), e);
        }
    }

    @Override
    public void remove(Long key) throws IllegalStateException, NoSuchElementException {
        checkOpen();
        validateKey(key);

        long position = key * objectSize;
        try {
            if (position >= dataFile.length()) {
                throw new NoSuchElementException("Key " + key + " does not exist");
            }

            // Check if already deleted
            dataFile.seek(position);
            if (dataFile.read() == DELETED) {
                throw new NoSuchElementException("Key " + key + " has already been deleted");
            }

            // Mark as deleted
            dataFile.seek(position);
            dataFile.writeByte(DELETED);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to remove key " + key + ": " + e.getMessage(), e);
        }
    }

    private void checkOpen() throws IllegalStateException {
        if (!isOpen) {
            throw new IllegalStateException("Container is not open");
        }
    }

    private void validateKey(Long key) throws IllegalArgumentException, NoSuchElementException {
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }
        if (key < 0) {
            throw new IllegalArgumentException("Key cannot be negative");
        }
        if (key >= recentKey) {
            throw new NoSuchElementException("Key " + key + " has not been reserved");
        }
    }
}