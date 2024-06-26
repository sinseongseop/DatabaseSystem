package databasesystem.hash;

public class Bucket {
    private final String key;
    private final int blockNum;
    private final int offset;
    private final int recordLength;

    public Bucket(String key, int blockNum, int offset, int recordLength) {
        this.key = key;
        this.blockNum = blockNum;
        this.offset = offset;
        this.recordLength = recordLength;
    }

    public String getKey() {
        return key;
    }

    public int getBlockNum() {
        return blockNum;
    }

    public int getOffset() {
        return offset;
    }

    public int getRecordLength() {
        return recordLength;
    }

}
