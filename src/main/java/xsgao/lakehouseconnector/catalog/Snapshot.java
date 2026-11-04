package xsgao.lakehouseconnector.catalog;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Metadata model for a table snapshot (data version).
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Snapshot {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    
    /**
     * Unique identifier for this snapshot.
     */
    @JsonProperty("snapshot-id")
    private long snapshotId;

    @JsonProperty("timestamp-ms")
    private long timestampMs;
    
    @JsonProperty("parent-snapshot-id")
    private Long parentSnapshotId;

    @JsonProperty ("added-files")
    private List<DataFile> addedFiles;

    public Snapshot() {
        this.snapshotId = 0;
        this.timestampMs = System.currentTimeMillis();
        this.parentSnapshotId = null;
        this.addedFiles = new java.util.ArrayList<>();
    }

    public Snapshot(long snapshotId, long timestampMs, Long parentSnapshotId, List<DataFile> addedFiles) {
        this.snapshotId = snapshotId;
        this.timestampMs = timestampMs;
        this.parentSnapshotId = parentSnapshotId;
        this.addedFiles = addedFiles;
    }

    public static Snapshot fromJson(String json) throws Exception {
        return OBJECT_MAPPER.readValue(json, Snapshot.class);
    }

    public String toJson() throws Exception {
        return OBJECT_MAPPER.writeValueAsString(this);
    }

    // Getters and Setters
    public long getSnapshotId() {
        return snapshotId;
    }

    public void setSnapshotId(long snapshotId) {
        this.snapshotId = snapshotId;
    }

    public Long getParentSnapshotId() {
        return parentSnapshotId;
    }

    public void setTimestampMs(long timestampMs) {
        this.timestampMs = timestampMs;
    }

    public void setParentSnapshotId(Long parentSnapshotId) {
        this.parentSnapshotId = parentSnapshotId;
    }

    public long getTimestampMs() {
        return timestampMs;
    }

    public List<DataFile> getAddedFiles() {
        return addedFiles;
    }

    public void setAddedFiles(List<DataFile> addedFiles) {
        this.addedFiles = addedFiles;
    }

    @Override 
    public String toString() {
        StringBuilder sb = new StringBuilder("Snapshot{");
        sb.append("snapshotId=").append(snapshotId);
        sb.append(", timestampMs=").append(timestampMs);
        sb.append(", parentSnapshotId=").append(parentSnapshotId);
        sb.append(", addedFiles=").append(addedFiles);
        sb.append("}");
        return sb.toString();
    }
}
