package xsgao.lakehouseconnector.catalog;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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
     * Full schema history, including the current schema. Legacy metadata has only {@code schema}.
     */
    @JsonProperty("schemas")
    private List<Schema> schemas;

    @JsonProperty("current-schema-id")
    private Long currentSchemaId;

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
        this.schemas = new ArrayList<>();
        this.currentSchemaId = null;
        this.currentSnapshotId = null;
        this.snapshots = new ArrayList<>();
    }

    public TableMetadata(int formatVersion, String tableUuid, String location, Schema schema, Long currentSnapshotId, List<Snapshot> snapshots) {
        this.formatVersion = formatVersion;
        this.tableUuid = tableUuid;
        this.location = location;
        this.schema = schema;
        this.schemas = schema == null ? new ArrayList<>() : new ArrayList<>(List.of(schema));
        this.currentSchemaId = schema == null ? null : schema.getSchemaId();
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

    public List<Schema> getSchemas() {
        if (schemas == null || schemas.isEmpty()) {
            return schema == null ? List.of() : List.of(schema);
        }
        return schemas;
    }

    public void setSchemas(List<Schema> schemas) {
        this.schemas = schemas;
    }

    public Long getCurrentSchemaId() {
        return currentSchemaId != null ? currentSchemaId : (schema == null ? null : schema.getSchemaId());
    }

    public void setCurrentSchemaId(Long currentSchemaId) {
        this.currentSchemaId = currentSchemaId;
    }

    /**
     * Resolves a schema id against the history. Legacy metadata without {@code schemas}
     * falls back to the single {@code schema} field.
     */
    public Schema findSchema(Long schemaId) {
        if (schemaId == null) {
            return schema;
        }
        for (Schema candidate : getSchemas()) {
            if (candidate.getSchemaId() == schemaId) {
                return candidate;
            }
        }
        return schema;
    }

    /**
     * Resolves the snapshot referenced by {@code current-snapshot-id}. Not a JavaBean getter,
     * so Jackson does not serialize this derived value.
     */
    public Snapshot findActiveSnapshot() {
        if (currentSnapshotId == null || snapshots == null) {
            return null;
        }
        return snapshots.stream()
                .filter(snapshot -> snapshot.getSnapshotId() == currentSnapshotId)
                .findFirst()
                .orElseThrow(() -> new IllegalStateException(
                        "Active snapshot ID " + currentSnapshotId + " not found."));
    }

    /**
     * Replays snapshot deltas up to the active snapshot. Legacy snapshots whose
     * {@code added-files} held the full live set still resolve correctly because files are
     * keyed by path and later entries overwrite earlier ones.
     */
    public List<DataFile> resolveLiveFiles() {
        Snapshot active = findActiveSnapshot();
        if (active == null || snapshots == null) {
            return List.of();
        }
        Map<String, DataFile> live = new LinkedHashMap<>();
        for (Snapshot snapshot : snapshots) {
            for (DataFile removed : snapshot.getRemovedFiles()) {
                live.remove(removed.getFilePath());
            }
            if (snapshot.getAddedFiles() != null) {
                for (DataFile added : snapshot.getAddedFiles()) {
                    live.put(added.getFilePath(), added);
                }
            }
            if (snapshot.getSnapshotId() == active.getSnapshotId()) {
                break;
            }
        }
        return List.copyOf(live.values());
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
        sb.append(", schemas=").append(schemas);
        sb.append(", currentSchemaId=").append(currentSchemaId);
        sb.append(", currentSnapshotId=").append(currentSnapshotId);
        sb.append(", snapshots=").append(snapshots);
        sb.append('}');
        return sb.toString();
    }
}
