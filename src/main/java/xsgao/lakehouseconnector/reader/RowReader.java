package xsgao.lakehouseconnector.reader;

import xsgao.lakehouseconnector.catalog.Field;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Reads one decoded record at a time independently of the underlying file format.
 */
public interface RowReader extends AutoCloseable {
    Map<String, Object> readNextRecord() throws IOException;

    /**
     * Fields this reader emits, in batch order. The scan operator uses this as the
     * RecordBatch schema, so the projection is defined once, here.
     */
    List<Field> getFields();

    @Override
    void close() throws IOException;
}
