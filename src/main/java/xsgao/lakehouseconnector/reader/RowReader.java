package xsgao.lakehouseconnector.reader;

import java.io.IOException;
import java.util.Map;

/**
 * Reads one decoded record at a time independently of the underlying file format.
 */
public interface RowReader extends AutoCloseable {
    Map<String, Object> readNextRecord() throws IOException;

    @Override
    void close() throws IOException;
}
