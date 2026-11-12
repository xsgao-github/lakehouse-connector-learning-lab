package xsgao.lakehouseconnector.vector;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.memory.ArrowBuf;

public class ColumnVectorWrapper {
    private final FieldVector vector;

    public ColumnVectorWrapper(FieldVector vector) {
        this.vector = vector;
    }

    public FieldVector getUnderlyingVector() {
        return vector;
    }

    public String getName() {
        return vector.getName();
    }

    public int getValueCount() {
        return vector.getValueCount();
    }

    public boolean isNull(int index) {
        return vector.isNull(index);
    }

    public ArrowBuf getDataBuffer() {
        return vector.getDataBuffer();
    }

    public ArrowBuf getValidityBuffer() {
        return vector.getValidityBuffer();
    }

    // Convenience typed accessors for execution operators
    public int getInt(int index) {
        if (vector instanceof IntVector) {
            return ((IntVector) vector).get(index);
        }
        throw new UnsupportedOperationException("Vector is not an IntVector: " + vector.getClass().getName());
    }

    public long getLong(int index) {
        if (vector instanceof BigIntVector) {
            return ((BigIntVector) vector).get(index);
        }
        throw new UnsupportedOperationException("Vector is not a BigIntVector: " + vector.getClass().getName());
    }
}