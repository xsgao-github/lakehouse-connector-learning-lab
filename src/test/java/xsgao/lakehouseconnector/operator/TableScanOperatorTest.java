package xsgao.lakehouseconnector.operator;

import org.junit.jupiter.api.Test;
import xsgao.lakehouseconnector.catalog.Field;
import xsgao.lakehouseconnector.model.RecordBatch;
import xsgao.lakehouseconnector.reader.RowReader;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TableScanOperatorTest {

    @Test
    void convertsRowsFromInjectedReaderIntoRecordBatches() {
        List<Field> fields = List.of(new Field(1, "id", "int", true));
        InMemoryRowReader rowReader = new InMemoryRowReader(List.of(
            Map.of("id", 1),
            Map.of("id", 2)
        ));
        TableScanOperator operator = new TableScanOperator(rowReader, fields);

        assertTrue(operator.hasNext());
        RecordBatch batch = operator.next();

        assertEquals(2, batch.getRowCount());
        assertEquals(1, batch.getRow(0).getValue("id"));
        assertEquals(2, batch.getRow(1).getValue("id"));
        assertFalse(operator.hasNext());
        assertTrue(rowReader.closed);
    }

    private static class InMemoryRowReader implements RowReader {
        private final Iterator<Map<String, Object>> records;
        private boolean closed;

        private InMemoryRowReader(List<Map<String, Object>> records) {
            this.records = records.iterator();
        }

        @Override
        public Map<String, Object> readNextRecord() {
            return records.hasNext() ? records.next() : null;
        }

        @Override
        public void close() throws IOException {
            closed = true;
        }
    }
}
