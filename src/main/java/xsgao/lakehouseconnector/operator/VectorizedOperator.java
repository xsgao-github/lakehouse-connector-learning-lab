package xsgao.lakehouseconnector.operator;

import org.apache.arrow.memory.BufferAllocator;
import xsgao.lakehouseconnector.vector.VectorBatch;

/**
 * Base interface for all vectorized query execution operators in Module 9.
 * Operators consume and produce VectorBatches in a pipeline architecture.
 */
public interface VectorizedOperator extends AutoCloseable {

    /**
     * Opens the operator, initializing internal state or resource allocations.
     */
    void open();

    /**
     * Processes the next vector batch through the operator kernel.
     * 
     * @param inputBatch The incoming VectorBatch from a child operator or table scan.
     * @param allocator  The off-heap BufferAllocator to manage output memory.
     * @return A processed VectorBatch resulting from the operator's execution.
     */
    VectorBatch next(VectorBatch inputBatch, BufferAllocator allocator);

    /**
     * Closes the operator, releasing any active execution resources.
     */
    @Override
    void close();
}