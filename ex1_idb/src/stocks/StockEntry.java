package stocks;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Represents a stock entry with an ID, name, timestamp, and market value.
 */
public class StockEntry {
    private final long id;
    private final String name;
    private final long ts;
    private final double value;

    /**
     * Constructs a StockEntry with the specified parameters.
     *
     * @param id the ID of the stock entry
     * @param name the name of the stock
     * @param timestamp the timestamp of the stock entry
     * @param market_value the market value of the stock
     * @throws IllegalArgumentException if the name is null or empty
     */
    public StockEntry(long id, String name, long timestamp, double market_value) {
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("Name cannot be null or empty");
        }
        this.id = id;
        this.name = name;
        this.ts = timestamp;
        this.value = market_value;
    }

    /**
     * Constructs a StockEntry from the ByteBuffer.
     *
     * @param bb the ByteBuffer containing the stock entry data
     */
    public StockEntry(ByteBuffer bb) {
        // TODO
        this.id = bb.getLong();
        short nameLength = bb.getShort();
        byte [] nameBytes = new byte[nameLength];
        bb.get(nameBytes);
        this.name = new String(nameBytes, StandardCharsets.UTF_8);
        this.ts = bb.getLong();
        this.value = bb.getDouble();
    }

    public long getId() {
        return this.id;
    }

    public String getName() {
        return this.name;
    }

    public long getTimeStamp() {
        return this.ts;
    }

    public double getMarketValue() {
        return this.value;
    }

    public int getSerializedLength() {
        return 3 * 8 + 2 + name.getBytes().length;
    }

    @Override
    public String toString() {
        return id + " " + name + " " + ts + " " + value;
    }

    /**
     * Converts the StockEntry to a ByteBuffer.
     *
     * @return the ByteBuffer containing the stock entry data
     */
    public ByteBuffer getBytes() {
        // TODO
        byte[] nameBytes = name.getBytes(StandardCharsets.UTF_8); // get name bytes
        int totalSize = Configuration.RECORD_SIZE + nameBytes.length; // total size of the record with the name string

        ByteBuffer bb = ByteBuffer.allocate(totalSize);

        bb.putLong(this.id);
        bb.putShort((short)nameBytes.length);
        bb.put(nameBytes);
        bb.putLong(this.ts);
        bb.putDouble(this.value);

        bb.flip(); // switch to read mode
        return bb;
    }

    public boolean equals(Object obj) {
        if (obj instanceof StockEntry) {
            StockEntry entry = (StockEntry) obj;
            return id == entry.id && name.equals(entry.name) && ts == entry.ts && value == entry.value;
        }
        return false;
    }
}
