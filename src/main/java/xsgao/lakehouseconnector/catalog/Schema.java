package xsgao.lakehouseconnector.catalog;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Metadata model for a table schema (list of columns with name and type).
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Schema {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    
    @JsonProperty("schema-id")
    private long schemaId;
    /**
     * List of column definitions in this schema.
     */
    @JsonProperty("fields")
    private List<Field> fields;

    public Schema() {
        this.schemaId = 0;
        this.fields = new java.util.ArrayList<>();
    }

    public Schema(long schemaId, List<Field> fields) {
        this.schemaId = schemaId;
        this.fields = fields;
    }

    public static Schema fromJson(String json) throws Exception {
        return OBJECT_MAPPER.readValue(json, Schema.class);
    }

    public String toJson() throws Exception {
        return OBJECT_MAPPER.writeValueAsString(this);
    }

    // Getters and Setters

    public long getSchemaId() {
        return schemaId;
    }

    public void setSchemaId(long schemaId) {
        this.schemaId = schemaId;
    }

    public List<Field> getFields() {
        return fields;
    }

    public void setFields(List<Field> fields) {
        this.fields = fields;
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("Schema{");
        sb.append("schemaId=").append(schemaId).append(", fields=[");
        for (Field f : fields) {
            sb.append(f).append(",");
        }
        if (!fields.isEmpty()) {
            sb.deleteCharAt(sb.length() - 1); // Remove trailing comma
        }
        sb.append("]}");
        return sb.toString();
    }
}
