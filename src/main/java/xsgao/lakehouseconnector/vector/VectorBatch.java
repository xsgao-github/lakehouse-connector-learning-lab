package xsgao.lakehouseconnector.vector;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * One columnar batch of Arrow vectors, the unit exchanged between vectorized operators.
 * Closing the batch releases the underlying {@link VectorSchemaRoot}.
 */
public class VectorBatch implements AutoCloseable {
    private final VectorSchemaRoot root;

    public VectorBatch(VectorSchemaRoot root) {
        this.root = root;
    }

    public VectorSchemaRoot getRoot() {
        return root;
    }

    public int getRowCount() {
        return root.getRowCount();
    }

    public Schema getSchema() {
        return root.getSchema();
    }

    @Override
    public void close() {
        root.close();
    }
}
