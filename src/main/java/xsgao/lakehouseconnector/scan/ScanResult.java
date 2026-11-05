package xsgao.lakehouseconnector.scan;

/**
 * Represents the outcome and metrics of scanning a single TableSplit 
 * during parallel data plane execution.
 */
public class ScanResult {
    private final String filePath;
    private final long recordCount;
    private final long bytesRead;
    private final long executionTimeMs;

    public ScanResult(String filePath, long recordCount) {
        this(filePath, recordCount, 0L, 0L);
    }

    public ScanResult(String filePath, long recordCount, long bytesRead, long executionTimeMs) {
        this.filePath = filePath;
        this.recordCount = recordCount;
        this.bytesRead = bytesRead;
        this.executionTimeMs = executionTimeMs;
    }

    public String getFilePath() {
        return filePath;
    }

    public long getRecordCount() {
        return recordCount;
    }

    public long getBytesRead() {
        return bytesRead;
    }

    public long getExecutionTimeMs() {
        return executionTimeMs;
    }

    @Override
    public String toString() {
        return "ScanResult{" +
                "filePath='" + filePath + '\'' +
                ", recordCount=" + recordCount +
                ", bytesRead=" + bytesRead +
                ", executionTimeMs=" + executionTimeMs +
                '}';
    }
}