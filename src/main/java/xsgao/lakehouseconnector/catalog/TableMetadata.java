package xsgao.lakehouseconnector.catalog;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Metadata model for a table in the storage-centric catalog.
 * Uses versioned JSON metadata design for table schema evolution support.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class TableMetadata {
    
    // Jackson ObjectMapper singleton for consistent serialization
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    /**
     * Format version identifier for this metadata schema.
     */
   @JsonProperty("format-version")
    private int formatVersion;
    
    /**
     * UUID uniquely identifying this table instance.
     */
    @JsonProperty ("table-uuid")
    private String tableUuid;
    
    /**
     * Physical storage location of the table data files.
     */
    @JsonProperty("location")
    private String location;
    
    /**
     * The current active schema (list of columns).
     */
    @JsonProperty("schema")
    private Schema schema;
    
    /**
     * ID of the most recent snapshot.
     */
    @JsonProperty("current-snapshot-id")
    private Long currentSnapshotId;
    
    /**
     * Historical snapshots for data versioning and lineage tracking.
     */
    @JsonProperty("snapshots")
    private List<Snapshot> snapshots;

    public TableMetadata() {
        this.formatVersion = 0;
        this.tableUuid = null;
        this.location = null;
        this.schema = null;
        this.currentSnapshotId = null;
        this.snapshots = new ArrayList<>();
    }

    public TableMetadata(int formatVersion, String tableUuid, String location, Schema schema, Long currentSnapshotId, List<Snapshot> snapshots) {
        this.formatVersion = formatVersion;
        this.tableUuid = tableUuid;
        this.location = location;
        this.schema = schema;
        this.currentSnapshotId = currentSnapshotId;
        this.snapshots = snapshots;
    }

    public static TableMetadata fromJson(String json) throws IOException {
        return OBJECT_MAPPER.readValue(json, TableMetadata.class);
    }

    public String toJson() {
        try {
            return OBJECT_MAPPER.writeValueAsString(this);
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize TableMetadata", e);
        }
    }

    // Getters and Setters
    public int getFormatVersion() {
        return formatVersion;
    }

    public void setFormatVersion(int formatVersion) {
        this.formatVersion = formatVersion;
    }

    public String getTableUuid() {
        return tableUuid;
    }

    public void setTableUuid(String tableUuid) {
        this.tableUuid = tableUuid;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public Schema getSchema() {
        return schema;
    }

    public void setSchema(Schema schema) {
        this.schema = schema;
    }

    public Long getCurrentSnapshotId() {
        return currentSnapshotId;
    }

    public void setCurrentSnapshotId(Long currentSnapshotId) {
        this.currentSnapshotId = currentSnapshotId;
    }

    public List<Snapshot> getSnapshots() {
        return snapshots;
    }

    public void setSnapshots(List<Snapshot> snapshots) {
        this.snapshots = snapshots;
    }

    @Override 
    public String toString() {
        StringBuilder sb = new StringBuilder("TableMetadata{");
        sb.append("formatVersion=").append(formatVersion);
        sb.append(", tableUuid='").append(tableUuid).append('\'');
        sb.append(", location='").append(location).append('\'');
        sb.append(", schema=").append(schema);
        sb.append(", currentSnapshotId=").append(currentSnapshotId);
        sb.append(", snapshots=").append(snapshots);
        sb.append('}');
        return sb.toString();
    }
}
