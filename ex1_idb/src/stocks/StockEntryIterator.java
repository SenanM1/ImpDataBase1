package stocks;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Iterator;

public class StockEntryIterator implements Iterator<StockEntry> {

    private long pos;
    private final RandomAccessFile file;

    public StockEntryIterator(RandomAccessFile file) {
        // TODO
        this.file = null;
    }

    public boolean hasNext() {
        // TODO
        return false;
    }

    public StockEntry next() {
        // TODO
        return null;
    }
}
