package xsgao.lakehouseconnector.parquet;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReader;

/**
 * Fallback decoder: per-value {@link ColumnReader} access. It handles every Parquet
 * encoding (PLAIN, dictionary, delta, ...) and optional/repeated columns, but does not
 * decode in bulk.
 */
final class ColumnReaderDecoder implements ColumnDecoder {

    private final ColumnDescriptor descriptor;
    private final ColumnReader reader;

    ColumnReaderDecoder(ColumnDescriptor descriptor, ColumnReader reader) {
        this.descriptor = descriptor;
        this.reader = reader;
    }

    @Override
    public void readRows(FieldVector vector, int startRow, int rows) {
        int maxDefinitionLevel = descriptor.getMaxDefinitionLevel();

        for (int i = 0; i < rows; i++) {
            int row = startRow + i;
            if (reader.getCurrentDefinitionLevel() == maxDefinitionLevel) {
                if (vector instanceof IntVector v) {
                    v.setSafe(row, reader.getInteger());
                } else if (vector instanceof BigIntVector v) {
                    v.setSafe(row, reader.getLong());
                } else if (vector instanceof Float4Vector v) {
                    v.setSafe(row, reader.getFloat());
                } else if (vector instanceof Float8Vector v) {
                    v.setSafe(row, reader.getDouble());
                } else if (vector instanceof BitVector v) {
                    v.setSafe(row, reader.getBoolean() ? 1 : 0);
                } else if (vector instanceof VarCharVector v) {
                    v.setSafe(row, reader.getBinary().getBytes());
                } else {
                    throw new UnsupportedOperationException(
                            "Vector type not supported for vectorized reading: " + vector.getClass().getName());
                }
            } else {
                vector.setNull(row);
            }
            reader.consume();
        }
    }
}
