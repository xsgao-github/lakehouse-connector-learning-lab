package xsgao.lakehouseconnector.catalog;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Represents column-level statistics (min value, max value, and null counts)
 * embedded within metadata snapshots for statistics-based pruning.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ColumnStats {
    @JsonProperty("lower-bound")
    private String lowerBound; // Stored as string/comparable for generic JSON mapping

    @JsonProperty("upper-bound")
    private String upperBound;

    @JsonProperty ("null-count")
    private long nullCount;

    public ColumnStats() {}

    public ColumnStats(String lowerBound, String upperBound, long nullCount) {
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
        this.nullCount = nullCount;
    }

    public String getLowerBound() {
        return lowerBound;
    }

    public void setLowerBound(String lowerBound) {
        this.lowerBound = lowerBound;
    }

    public String getUpperBound() {
        return upperBound;
    }

    public void setUpperBound(String upperBound) {
        this.upperBound = upperBound;
    }

    public long getNullCount() {
        return nullCount;
    }

    public void setNullCount(long nullCount) {
        this.nullCount = nullCount;
    }

    @Override
    public String toString() {
        return "[" + lowerBound + ", " + upperBound + ", " + nullCount + "]";
    }
}
