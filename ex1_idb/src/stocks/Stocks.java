package stocks;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Class representing a collection of stock entries that are stored in a binary file.
 */
public class Stocks implements Iterable<StockEntry> {

    private static final int ID_SIZE = 8;
    private static final int NAME_LENGTH_SIZE = 2;
    private static final int TIMESTAMP_SIZE = 8;
    private static final int VALUE_SIZE = 8;
    private static final int RECORD_SIZE = ID_SIZE + NAME_LENGTH_SIZE + TIMESTAMP_SIZE + VALUE_SIZE;

    private final RandomAccessFile file;

    /**
     * Constructs a Stocks object with the specified file path.
     *
     * @param path is a path to the binary file
     * @throws FileNotFoundException if the file is not found or cannot be opened
     */
    Stocks(String path) throws FileNotFoundException {
        // TODO
        this.file = new RandomAccessFile(new File(path), "rw");
    }

    /**
     * Returns a stock entry at the given index.
     *
     * @param i the index of the stock entry
     * @return the stock entry at the specified index, or null if the entry is not found
     */
    public StockEntry get(int i) {
        // TODO
        try {
            file.seek(0);

            int ind = 0;

            while (ind <= i) {
                ByteBuffer bb = ByteBuffer.allocate(RECORD_SIZE);
                file.read(bb.array(), 0, ID_SIZE + NAME_LENGTH_SIZE); // read number of bytes for id and name length
                bb.position(0); // reset position

                long id = bb.getLong(); // read id
                short nameLength = bb.getShort(); // read name length

                byte[] nameBytes = new byte[nameLength];
                file.read(nameBytes); // read name
                String name = new String(nameBytes, StandardCharsets.UTF_8);

                bb = ByteBuffer.allocate(8 + 8); // allocate buffer for ts and value
                file.read(bb.array(), 0, 8 + 8); // read number of bytes for timestamp and value
                bb.position(0); // reset position

                long ts = bb.getLong(); // read timestamp
                double value = bb.getDouble(); // read value

                if (ind == i) {
                    return new StockEntry(id, name, ts, value); // if needed stock found return it
                }

                ind++;
            }
        } catch (IOException e) {
            System.err.println("Error getting stock entry");
        }

        return null;
    }

    @Override
    public Iterator<StockEntry> iterator() {
        return new StockEntryIterator(file);
    }
}