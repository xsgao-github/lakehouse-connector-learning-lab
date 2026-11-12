package xsgao.lakehouseconnector.parquet;

import org.apache.arrow.vector.FieldVector;

import java.io.IOException;

/**
 * Decodes one Parquet column chunk into an Arrow vector, row range by row range.
 * Implementations are stateful across calls and advance through pages themselves.
 */
interface ColumnDecoder {
    void readRows(FieldVector vector, int startRow, int rows) throws IOException;
}
