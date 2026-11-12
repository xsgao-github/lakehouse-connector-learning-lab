package xsgao.lakehouseconnector.operator;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.types.pojo.Field;
import xsgao.lakehouseconnector.vector.VectorBatch;

import java.util.ArrayList;
import java.util.List;

/**
 * Projection stage for computed or reordered columns.
 *
 * <p>Plain column pruning does not belong here: pass the projected Arrow schema to the
 * reader so excluded columns are never decoded. Use this operator when the projection
 * computes values (for example {@code quantity * price AS revenue}) or renames columns.
 */
public class VectorizedProjectOperator implements VectorizedOperator {

    private final List<String> projectedColumnNames;

    public VectorizedProjectOperator(List<String> projectedColumnNames) {
        this.projectedColumnNames = projectedColumnNames;
    }

    @Override
    public void open() {
        // Initialization logic if required
    }

    @Override
    public VectorBatch next(VectorBatch inputBatch, BufferAllocator allocator) {
        if (inputBatch == null) {
            return null;
        }

        VectorSchemaRoot inputRoot = inputBatch.getRoot();
        int rowCount = inputBatch.getRowCount();

        List<FieldVector> sourceVectors = new ArrayList<>();
        List<Field> projectedFields = new ArrayList<>();

        for (String colName : projectedColumnNames) {
            FieldVector vector = inputRoot.getVector(colName);
            if (vector == null) {
                throw new IllegalArgumentException("Projected column not found in batch: " + colName);
            }
            sourceVectors.add(vector);
            projectedFields.add(vector.getField());
        }

        Schema outputSchema = new Schema(projectedFields);
        VectorSchemaRoot outputRoot = VectorSchemaRoot.create(outputSchema, allocator);
        outputRoot.allocateNew();
        outputRoot.setRowCount(rowCount);

        // Copy the projected columns row by row. A production engine would use
        // TransferPair when the input batch is not reused, moving buffers instead of copying.
        for (int i = 0; i < sourceVectors.size(); i++) {
            FieldVector srcVector = sourceVectors.get(i);
            FieldVector dstVector = outputRoot.getVector(i);

            for (int row = 0; row < rowCount; row++) {
                dstVector.copyFrom(row, row, srcVector);
            }
            dstVector.setValueCount(rowCount);
        }

        return new VectorBatch(outputRoot);
    }

    @Override
    public void close() {
        // Release execution resources if needed
    }
}