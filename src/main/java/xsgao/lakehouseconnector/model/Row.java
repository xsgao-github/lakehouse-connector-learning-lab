package xsgao.lakehouseconnector.model;

import java.util.Map;

public class Row {
    private final Map<String, Object> values;

    public Row(Map<String, Object> values) {
        this.values = values;
    }

    public Object getValue(String columnName) {
        return values.get(columnName);
    }

    public String getString(String columnName) {
        Object val = getValue(columnName);
        return val == null ? null : String.valueOf(val);
    }

    public double getDouble(String columnName) {
        Object val = getValue(columnName);
        if (val instanceof Number) {
            return ((Number) val).doubleValue();
        }
        if (val instanceof String) {
            try {
                return Double.parseDouble((String) val);
            } catch (NumberFormatException e) {
                return 0.0;
            }
        }
        return 0.0;
    }

    public Map<String, Object> getValues() {
        return values;
    }
}
