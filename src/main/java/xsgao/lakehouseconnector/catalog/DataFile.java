package xsgao.lakehouseconnector.catalog;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Metadata model for a physical data file (e.g., Parquet file).
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

    public DataFile() {
        this.filePath = null;
        this.recordCount = 0;
        this.fileSizeInBytes = 0;
    }

    public DataFile(String filePath, long recordCount, long fileSizeInBytes) {
        this.filePath = filePath;
        this.recordCount = recordCount;
        this.fileSizeInBytes = fileSizeInBytes;
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

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("DataFile{");
        sb.append("filePath='").append(filePath).append('\'');
        sb.append(", recordCount=").append(recordCount);
        sb.append(", fileSizeInBytes=").append(fileSizeInBytes);
        sb.append("}");
        return sb.toString();
    }

}
