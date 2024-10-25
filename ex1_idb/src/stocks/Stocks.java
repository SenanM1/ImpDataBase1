package stocks;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.*;

public class Stocks implements Iterable<StockEntry> {

    private final RandomAccessFile file;

    Stocks(String path) throws FileNotFoundException {
        // TODO
        file = null;
    }

    public StockEntry get(int i) {
        // TODO
        return null;
    }

    @Override
    public Iterator<StockEntry> iterator() {
        return new StockEntryIterator(file);
    }
}