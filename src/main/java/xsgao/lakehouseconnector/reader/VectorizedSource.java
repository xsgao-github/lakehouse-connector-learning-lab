package xsgao.lakehouseconnector.reader;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Schema;
import xsgao.lakehouseconnector.vector.VectorBatch;

import java.io.IOException;

/**
 * Scan-stage contract for the vectorized pipeline: produces batches, has no upstream,
 * and owns its resource lifecycle. Mirrors {@link xsgao.lakehouseconnector.reader.RowReader}
 * on the row-based side.
 */
public interface VectorizedSource extends AutoCloseable {

    /**
     * @return the next batch (off-heap, allocated from the given allocator), or null at EOF.
     */
    VectorBatch nextBatch(BufferAllocator allocator) throws IOException;

    Schema getSchema();

    @Override
    void close() throws IOException;
}
