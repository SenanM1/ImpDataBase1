package stocks;

/**
 * Configuration class that holds constants for the sizes of various fields in a stock record.
 */
public class Configuration {
    public static final int ID_SIZE = 8;
    public static final int NAME_LENGTH_SIZE = 2;
    public static final int TIMESTAMP_SIZE = 8;
    public static final int VALUE_SIZE = 8;
    public static final int RECORD_SIZE = ID_SIZE + NAME_LENGTH_SIZE + TIMESTAMP_SIZE + VALUE_SIZE;
}
