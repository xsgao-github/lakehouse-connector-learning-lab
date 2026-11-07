package xsgao.lakehouseconnector.operator;

import xsgao.lakehouseconnector.catalog.Field;
import xsgao.lakehouseconnector.model.RecordBatch;
import xsgao.lakehouseconnector.model.Row;
import xsgao.lakehouseconnector.reader.RowReader;

import java.util.*;

public class TableScanOperator implements Iterator<RecordBatch> {
    private final RowReader rowReader;
    private final List<Field> projectedFields;
    private RecordBatch nextBatch = null;

    public TableScanOperator(RowReader rowReader, List<Field> projectedFields) {
        this.rowReader = rowReader;
        this.projectedFields = projectedFields;
        advance();
    }

    private void advance() {
        try {
            Map<String, Object> record;
            RecordBatch batch = new RecordBatch(projectedFields);

            // Read up to batch size or until reader is exhausted
            while (batch.getRowCount() < 1000 && (record = rowReader.readNextRecord()) != null) {
                batch.addRow(new Row(record));
            }

            if (batch.getRowCount() > 0) {
                nextBatch = batch;
            } else {
                nextBatch = null;
                rowReader.close();
            }
        } catch (Exception e) {
            try { rowReader.close(); } catch (Exception ignored) {}
            throw new RuntimeException("Error reading records", e);
        }
    }

    @Override
    public boolean hasNext() {
        return nextBatch != null;
    }

    @Override
    public RecordBatch next() {
        if (!hasNext()) {
            throw new NoSuchElementException("No more batches in scan operator.");
        }
        RecordBatch current = nextBatch;
        advance();
        return current;
    }
}
