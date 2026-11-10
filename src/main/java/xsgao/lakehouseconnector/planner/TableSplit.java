package xsgao.lakehouseconnector.planner;

public class TableSplit {
    private final String filePath;
    private final long recordCount;
    private final long fileSizeBytes;
    private final long schemaId;

    public TableSplit(String filePath, long recordCount, long fileSizeBytes, long schemaId) {
        this.filePath = filePath;
        this.recordCount = recordCount;
        this.fileSizeBytes = fileSizeBytes;
        this.schemaId = schemaId;
    }

    public String getFilePath() { return filePath; }
    public long getRecordCount() { return recordCount; }
    public long getFileSizeBytes() { return fileSizeBytes; }
    public long getSchemaId() { return schemaId; }

    @Override
    public String toString() {
        return String.format("TableSplit{filePath='%s', size=%d bytes, schemaId=%d}", filePath, fileSizeBytes, schemaId);
    }
}