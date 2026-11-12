package xsgao.lakehouseconnector.operator;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Schema;
import xsgao.lakehouseconnector.parquet.VectorizedParquetTableReader;
import xsgao.lakehouseconnector.reader.VectorizedSource;
import xsgao.lakehouseconnector.vector.VectorBatch;

import java.io.IOException;

/**
 * Scan stage of the vectorized pipeline. Wraps a {@link VectorizedParquetTableReader}, owns
 * the batch size, and exposes the source schema, mirroring {@link TableScanOperator}
 * on the row-based side.
 */
public class VectorizedTableScanOperator implements VectorizedSource {

    private final VectorizedParquetTableReader reader;
    private final int batchSize;

    public VectorizedTableScanOperator(VectorizedParquetTableReader reader, int batchSize) {
        this.reader = reader;
        this.batchSize = batchSize;
    }

    @Override
    public VectorBatch nextBatch(BufferAllocator allocator) throws IOException {
        return reader.nextBatch(allocator, batchSize);
    }

    @Override
    public Schema getSchema() {
        return reader.getSchema();
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }
}
