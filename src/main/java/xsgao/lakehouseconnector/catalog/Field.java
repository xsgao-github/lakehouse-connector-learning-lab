package xsgao.lakehouseconnector.catalog;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Metadata model for a table field (column).
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Field {
    // Jackson ObjectMapper singleton for consistent serialization
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    /**
     * Unique identifier for this field.
     */
    @JsonProperty("id")
    private int  id;
    
    /**
     * Column name.
     */
    @JsonProperty("name")
    private String name;
    
    /**
     * Column data type (e.g., "VARCHAR", "INT", "DOUBLE").
     */
    @JsonProperty("type")
    private String type;
    
    /**
     * Optional field description.
     */
    @JsonProperty("required")
    private boolean required;

    public Field() {
    }
   
    public Field(int id, String name, String type, boolean required) {
        this.id = id;
        this.name = name;
        this.type = type.toLowerCase();
        this.required = required;
    }

    public static Field fromJson(String json) throws Exception {
        return OBJECT_MAPPER.readValue(json, Field.class);
    }

    public String toJson() throws Exception {
        return OBJECT_MAPPER.writeValueAsString(this);
    }

    // Getters and Setters
    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type.toLowerCase();
    }

    public boolean isRequired() {
        return required;
    }

    public void setRequired(boolean required) {
        this.required = required;
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("Field{");
        sb.append("id=").append(id);
        sb.append(", name='").append(name).append('\'');
        sb.append(", type='").append(type).append('\'');
        sb.append(", required=").append(required);
        sb.append('}');
        return sb.toString();
    }
}
