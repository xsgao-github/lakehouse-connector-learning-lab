package xsgao.lakehouseconnector.model;

import java.util.ArrayList;
import java.util.List;
import xsgao.lakehouseconnector.catalog.Field;

public class RecordBatch {
    private final List<Field> fields;
    private final List<Row> rows;

    public RecordBatch(List<Field> fields) {
        this.fields = fields;
        this.rows = new ArrayList<>();
    }

    public void addRow(Row row) {
        rows.add(row);
    }

    public int getRowCount() {
        return rows.size();
    }

    public Row getRow(int index) {
        return rows.get(index);
    }

    public List<Field> getFields() {
        return fields;
    }

    public List<Row> getRows() {
        return rows;
    }
}
