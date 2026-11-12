package xsgao.lakehouseconnector.operator;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.BitVectorHelper;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import xsgao.lakehouseconnector.vector.VectorBatch;

import java.nio.charset.StandardCharsets;
import java.util.function.DoublePredicate;
import java.util.function.IntPredicate;
import java.util.function.LongPredicate;
import java.util.function.Predicate;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Vectorized filter operator: evaluates a typed predicate over one column, builds a
 * selection vector, then gathers matching rows column by column.
 *
 * <p>Fixed-width columns are gathered straight from Arrow data buffers; other types
 * fall back to {@code copyFrom}. Use the typed factories ({@link #ints}, {@link #longs},
 * {@link #reals}, {@link #strings}, {@link #booleans}) so predicates see primitives.
 */
public class VectorizedFilterOperator implements VectorizedOperator {

    private static final Logger LOGGER = Logger.getLogger(VectorizedFilterOperator.class.getName());

    @FunctionalInterface
    public interface ValuePredicate {
        boolean test(FieldVector vector, int row);
    }

    private final String targetColumnName;
    private final ValuePredicate predicate;

    public VectorizedFilterOperator(String targetColumnName, ValuePredicate predicate) {
        this.targetColumnName = targetColumnName;
        this.predicate = predicate;
    }

    public static ValuePredicate ints(IntPredicate predicate) {
        return (vector, row) -> predicate.test(((IntVector) vector).get(row));
    }

    public static ValuePredicate longs(LongPredicate predicate) {
        return (vector, row) -> predicate.test(((BigIntVector) vector).get(row));
    }

    /**
     * Covers both FLOAT (Float4Vector) and DOUBLE (Float8Vector).
     */
    public static ValuePredicate reals(DoublePredicate predicate) {
        return (vector, row) -> {
            if (vector instanceof Float4Vector v) {
                return predicate.test(v.get(row));
            }
            if (vector instanceof Float8Vector v) {
                return predicate.test(v.get(row));
            }
            throw new IllegalArgumentException("Not a floating-point vector: " + vector.getClass().getName());
        };
    }

    public static ValuePredicate strings(Predicate<String> predicate) {
        return (vector, row) ->
                predicate.test(new String(((VarCharVector) vector).get(row), StandardCharsets.UTF_8));
    }

    public static ValuePredicate booleans(Predicate<Boolean> predicate) {
        return (vector, row) -> predicate.test(((BitVector) vector).get(row) != 0);
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
        int inputRowCount = inputBatch.getRowCount();

        FieldVector targetVector = inputRoot.getVector(targetColumnName);
        if (targetVector == null) {
            throw new IllegalArgumentException("Column not found for filtering: " + targetColumnName);
        }

        // 1. Selection vector: one pass over the predicate column
        int[] selected = new int[inputRowCount];
        int count = 0;
        boolean targetHasNulls = targetVector.getNullCount() > 0;
        for (int i = 0; i < inputRowCount; i++) {
            if ((!targetHasNulls || !targetVector.isNull(i)) && predicate.test(targetVector, i)) {
                selected[count++] = i;
            }
        }

        // 2. Allocate the output with capacity for the selected rows
        VectorSchemaRoot outputRoot = VectorSchemaRoot.create(inputRoot.getSchema(), allocator);
        for (FieldVector vector : outputRoot.getFieldVectors()) {
            vector.setInitialCapacity(Math.max(count, 1));
        }
        outputRoot.allocateNew();

        // 3. Gather each column once, using the same selection
        for (int c = 0; c < inputRoot.getFieldVectors().size(); c++) {
            gather(inputRoot.getVector(c), outputRoot.getVector(c), selected, count);
        }

        outputRoot.setRowCount(count);
        if (LOGGER.isLoggable(Level.FINE)) {
            LOGGER.fine(String.format(
                    "vectorized filter selected %,d of %,d rows into an off-heap batch", count, inputRowCount));
        }
        return new VectorBatch(outputRoot);
    }

    private void gather(FieldVector source, FieldVector dest, int[] selected, int count) {
        if (source instanceof IntVector s && dest instanceof IntVector d) {
            ArrowBuf src = s.getDataBuffer();
            ArrowBuf dst = d.getDataBuffer();
            for (int o = 0; o < count; o++) {
                int row = selected[o];
                dst.setInt((long) o * Integer.BYTES, src.getInt((long) row * Integer.BYTES));
            }
            copyValidity(s, d, selected, count);
        } else if (source instanceof BigIntVector s && dest instanceof BigIntVector d) {
            ArrowBuf src = s.getDataBuffer();
            ArrowBuf dst = d.getDataBuffer();
            for (int o = 0; o < count; o++) {
                int row = selected[o];
                dst.setLong((long) o * Long.BYTES, src.getLong((long) row * Long.BYTES));
            }
            copyValidity(s, d, selected, count);
        } else if (source instanceof Float4Vector s && dest instanceof Float4Vector d) {
            ArrowBuf src = s.getDataBuffer();
            ArrowBuf dst = d.getDataBuffer();
            for (int o = 0; o < count; o++) {
                int row = selected[o];
                dst.setFloat((long) o * Float.BYTES, src.getFloat((long) row * Float.BYTES));
            }
            copyValidity(s, d, selected, count);
        } else if (source instanceof Float8Vector s && dest instanceof Float8Vector d) {
            ArrowBuf src = s.getDataBuffer();
            ArrowBuf dst = d.getDataBuffer();
            for (int o = 0; o < count; o++) {
                int row = selected[o];
                dst.setDouble((long) o * Double.BYTES, src.getDouble((long) row * Double.BYTES));
            }
            copyValidity(s, d, selected, count);
        } else if (source instanceof VarCharVector s && dest instanceof VarCharVector d) {
            gatherVarChar(s, d, selected, count);
            return; // gatherVarChar sets the value count before writing offsets
        } else {
            // Other types copy through the vector API.
            for (int o = 0; o < count; o++) {
                dest.copyFrom(selected[o], o, source);
            }
        }
        dest.setValueCount(count);
    }

    /**
     * Copies validity for the gathered rows. Required columns (no nulls) get one bulk
     * write of 0xFF bytes instead of a per-row bit set.
     */
    private void copyValidity(FieldVector source, FieldVector dest, int[] selected, int count) {
        ArrowBuf validity = dest.getValidityBuffer();
        if (source.getNullCount() == 0) {
            markAllValid(validity, count);
            return;
        }
        for (int o = 0; o < count; o++) {
            if (!source.isNull(selected[o])) {
                BitVectorHelper.setBit(validity, o);
            }
        }
    }

    private static void markAllValid(ArrowBuf validity, int count) {
        byte[] allValid = new byte[(count + 7) >>> 3];
        java.util.Arrays.fill(allValid, (byte) 0xFF);
        validity.setBytes(0, allValid, 0, allValid.length);
    }

    /**
     * Gathers variable-width values by writing offsets and data directly: one memcpy per
     * surviving value into a destination data buffer allocated once, instead of a
     * per-row {@code copyFrom} with its capacity bookkeeping.
     */
    private void gatherVarChar(VarCharVector source, VarCharVector dest, int[] selected, int count) {
        // setValueCount fills holes by zeroing offsets, so it must run before we write them.
        dest.setValueCount(count);

        ArrowBuf srcOffsets = source.getOffsetBuffer();
        ArrowBuf srcData = source.getDataBuffer();
        ArrowBuf dstOffsets = dest.getOffsetBuffer();
        ArrowBuf validity = dest.getValidityBuffer();

        boolean allValid = source.getNullCount() == 0;
        if (allValid) {
            markAllValid(validity, count);
        }

        long totalBytes = 0;
        for (int o = 0; o < count; o++) {
            int row = selected[o];
            if (allValid || !source.isNull(row)) {
                totalBytes += (long) srcOffsets.getInt((long) (row + 1) * Integer.BYTES)
                        - srcOffsets.getInt((long) row * Integer.BYTES);
            }
        }
        // reallocDataBuffer copies the old buffer into the new one, so it can only grow.
        if (totalBytes > dest.getDataBuffer().capacity()) {
            dest.reallocDataBuffer(totalBytes);
        }
        ArrowBuf dstData = dest.getDataBuffer();

        int position = 0;
        for (int o = 0; o < count; o++) {
            int row = selected[o];
            dstOffsets.setInt((long) o * Integer.BYTES, position);
            if (allValid || !source.isNull(row)) {
                int start = srcOffsets.getInt((long) row * Integer.BYTES);
                int length = srcOffsets.getInt((long) (row + 1) * Integer.BYTES) - start;
                dstData.setBytes(position, srcData, start, length);
                position += length;
                if (!allValid) {
                    BitVectorHelper.setBit(validity, o);
                }
            }
        }
        dstOffsets.setInt((long) count * Integer.BYTES, position);
    }

    @Override
    public void close() {
        // Release execution resources if needed
    }
}
