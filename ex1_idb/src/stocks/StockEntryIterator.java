package stocks;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Iterator;

/**
 * Iterator for StockEntry objects stored in a RandomAccessFile.
 */
public class StockEntryIterator implements Iterator<StockEntry> {

    private long pos;
    private final RandomAccessFile file;

    /**
     * Constructs a StockEntryIterator for the given RandomAccessFile.
     *
     * @param file the RandomAccessFile containing StockEntry objects
     */
    public StockEntryIterator(RandomAccessFile file) {
        // TODO
        this.file = file;
        this.pos = 0;
    }

    /**
     * Checks if there is another StockEntry in the file.
     *
     * @return true if there is another StockEntry, false otherwise
     */
    public boolean hasNext() {
        // TODO
        try {
            return pos < file.length(); // checks if the current position is less than the length of the file
        } catch (IOException e) {
            System.err.println("Error checking if there is a next element: " + e.getMessage());
        }
        return false;
    }

    /**
     * Retrieves the next StockEntry from the file.
     *
     * @return the next StockEntry, or null if an error occurs
     */
    public StockEntry next() {
        // TODO
        try {
            file.seek(pos);

            // read id, name length, name, timestamp, and value
            long id = file.readLong();
            short nameLength = file.readShort();
            byte[] nameBytes = new byte[nameLength];
            file.read(nameBytes);
            String name = new String(nameBytes);
            long ts = file.readLong();
            double value = file.readDouble();

            pos = file.getFilePointer(); // updates the current position

            return new StockEntry(id, name, ts, value);
        } catch (IOException e) {
            System.err.println("Error getting next stock entry: " + e.getMessage());
        }
        return null;
    }
}
