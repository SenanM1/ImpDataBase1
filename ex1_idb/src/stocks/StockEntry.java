package stocks;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class StockEntry {
    private final long id;
    private final String name;
    private final long ts;
    private final double value;

    public StockEntry(long id, String name, long timestamp, double market_value) {
        this.id = id;
        this.name = name;
        this.ts = timestamp;
        this.value = market_value;
    }

    public StockEntry(ByteBuffer bb) {
        // TODO
        this.id = bb.getInt();
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

    public ByteBuffer getBytes() {
        // TODO
        byte[] nameBytes = name.getBytes(StandardCharsets.UTF_8);
        // Accounts for ID, name length, and timestamp (each 4 bytes).
        int size = Integer.BYTES * 3 + Double.BYTES + nameBytes.length;

        ByteBuffer bb = ByteBuffer.allocate(size);

        bb.putLong(id);
        bb.putShort((short)nameBytes.length);
        bb.put(nameBytes);
        bb.putLong(ts);
        bb.putDouble(value);

        bb.flip();
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
