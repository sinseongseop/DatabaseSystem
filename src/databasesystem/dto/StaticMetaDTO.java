package databasesystem.dto;

public class StaticMetaDTO {
    private final int recordSize;
    private final int recordCount;

    public StaticMetaDTO(int recordSize, int recordCount) {
        this.recordSize = recordSize;
        this.recordCount = recordCount;
    }

    public int getRecordSize() {
        return recordSize;
    }

    public int getRecordCount() {
        return recordCount;
    }
}
