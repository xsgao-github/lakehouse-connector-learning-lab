package xsgao.lakehouseconnector.catalog;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Metadata model for a physical data file (e.g., Parquet file) with column statistics.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class DataFile {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @JsonProperty("file-path")
    private String filePath;
    
    @JsonProperty("record-count")
    private long recordCount;
    
    @JsonProperty("file-size-in-bytes")
    private long fileSizeInBytes;

    /**
     * Column-level statistics mapping column name to its min/max bounds.
     */
    @JsonProperty("column-stats")
    private Map<String, ColumnStats> columnStats;

    /**
     * Schema id of the schema the file was written with. Null on files written before
     * schema history was tracked; readers then fall back to the current schema.
     */
    @JsonProperty("schema-id")
    private Long schemaId;

    public DataFile() {
        this.filePath = null;
        this.recordCount = 0;
        this.fileSizeInBytes = 0;
        this.columnStats = null;
        this.schemaId = null;
    }

    public DataFile(String filePath, long recordCount, long fileSizeInBytes) {
        this.filePath = filePath;
        this.recordCount = recordCount;
        this.fileSizeInBytes = fileSizeInBytes;
        this.columnStats = null;
        this.schemaId = null;
    }

    public DataFile(String filePath, long recordCount, long fileSizeInBytes, Map<String, ColumnStats> columnStats) {
        this.filePath = filePath;
        this.recordCount = recordCount;
        this.fileSizeInBytes = fileSizeInBytes;
        this.columnStats = columnStats;
        this.schemaId = null;
    }

    public DataFile(String filePath, long recordCount, long fileSizeInBytes,
                    Map<String, ColumnStats> columnStats, Long schemaId) {
        this.filePath = filePath;
        this.recordCount = recordCount;
        this.fileSizeInBytes = fileSizeInBytes;
        this.columnStats = columnStats;
        this.schemaId = schemaId;
    }

    public static DataFile fromJson(String json) throws Exception {
        return OBJECT_MAPPER.readValue(json, DataFile.class);
    }

    public String toJson() throws Exception {
        return OBJECT_MAPPER.writeValueAsString(this);
    }

    // Getters and Setters

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    public long getRecordCount() {
        return recordCount;
    }

    public void setRecordCount(long recordCount) {
        this.recordCount = recordCount;
    }

    public long getFileSizeInBytes() {
        return fileSizeInBytes;
    }

    public void setFileSizeInBytes(long fileSizeInBytes) {
        this.fileSizeInBytes = fileSizeInBytes;
    }

    public Map<String, ColumnStats> getColumnStats() {
        return columnStats;
    }

    public void setColumnStats(Map<String, ColumnStats> columnStats) {
        this.columnStats = columnStats;
    }

    public Long getSchemaId() {
        return schemaId;
    }

    public void setSchemaId(Long schemaId) {
        this.schemaId = schemaId;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("DataFile{");
        sb.append("filePath='").append(filePath).append('\'');
        sb.append(", recordCount=").append(recordCount);
        sb.append(", fileSizeInBytes=").append(fileSizeInBytes);
        sb.append(", columnStats=").append(columnStats);
        sb.append(", schemaId=").append(schemaId);
        sb.append("}");
        return sb.toString();
    }

}