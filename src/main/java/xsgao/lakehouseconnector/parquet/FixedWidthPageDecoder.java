package xsgao.lakehouseconnector.parquet;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVectorHelper;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.DataPageV2;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.schema.PrimitiveType;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Bulk decoder for required flat fixed-width columns (INT32, INT64, FLOAT, DOUBLE)
 * encoded with PLAIN or dictionary pages.
 *
 * <p>PLAIN pages are decoded into contiguous primitive arrays with one {@link ByteBuffer}
 * view; dictionary pages decode indices with the bulk RLE/bit-packing decoder. Values are
 * then copied into Arrow buffers in one byte copy per batch slice.
 */
final class FixedWidthPageDecoder implements ColumnDecoder {

    private final ColumnDescriptor descriptor;
    private final PageReader pageReader;
    private final PrimitiveType.PrimitiveTypeName type;

    private Object dictionary;
    private Object values;
    private int valueIndex;
    private int valueCount;

    FixedWidthPageDecoder(ColumnDescriptor descriptor, PageReader pageReader) throws IOException {
        this.descriptor = descriptor;
        this.pageReader = pageReader;
        this.type = descriptor.getPrimitiveType().getPrimitiveTypeName();

        DictionaryPage dictionaryPage = pageReader.readDictionaryPage();
        if (dictionaryPage != null) {
            this.dictionary = decodePlain(dictionaryPage.getBytes(), dictionaryPage.getDictionarySize());
        }
    }

    @Override
    public void readRows(FieldVector vector, int startRow, int rows) throws IOException {
        int out = startRow;
        int remaining = rows;

        while (remaining > 0) {
            if (values == null || valueIndex >= valueCount) {
                loadNextPage();
            }
            int n = Math.min(remaining, valueCount - valueIndex);
            copyToVector(vector, out, valueIndex, n);
            valueIndex += n;
            out += n;
            remaining -= n;
        }

        // Required columns have no nulls; mark every written value valid in bulk.
        ArrowBuf validity = vector.getValidityBuffer();
        if (startRow == 0) {
            byte[] allValid = new byte[(rows + 7) >>> 3];
            java.util.Arrays.fill(allValid, (byte) 0xFF);
            validity.setBytes(0, allValid, 0, allValid.length);
        } else {
            for (int i = startRow; i < startRow + rows; i++) {
                BitVectorHelper.setBit(validity, i);
            }
        }
    }

    private void loadNextPage() throws IOException {
        DataPage page = pageReader.readPage();
        if (page == null) {
            throw new IOException("Unexpected end of pages for column " + String.join(".", descriptor.getPath()));
        }

        Encoding encoding;
        BytesInput bytes;
        if (page instanceof DataPageV1 v1) {
            encoding = v1.getValueEncoding();
            // Required flat column: repetition and definition level sections are empty.
            bytes = v1.getBytes();
        } else if (page instanceof DataPageV2 v2) {
            encoding = v2.getDataEncoding();
            bytes = v2.getData();
        } else {
            throw new IOException("Unsupported page type: " + page.getClass().getName());
        }

        valueCount = page.getValueCount();
        valueIndex = 0;

        if (encoding == Encoding.PLAIN) {
            values = decodePlain(bytes, valueCount);
        } else if (encoding == Encoding.PLAIN_DICTIONARY || encoding == Encoding.RLE_DICTIONARY) {
            if (dictionary == null) {
                throw new IOException("Dictionary-encoded page without dictionary: "
                        + String.join(".", descriptor.getPath()));
            }
            values = decodeDictionary(bytes, valueCount);
        } else {
            throw new IOException("Unsupported encoding " + encoding + " for column "
                    + String.join(".", descriptor.getPath()));
        }
    }

    private Object decodePlain(BytesInput input, int count) throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(input.toByteArray()).order(ByteOrder.LITTLE_ENDIAN);
        switch (type) {
            case INT32: {
                int[] decoded = new int[count];
                buffer.asIntBuffer().get(decoded);
                return decoded;
            }
            case INT64: {
                long[] decoded = new long[count];
                buffer.asLongBuffer().get(decoded);
                return decoded;
            }
            case FLOAT: {
                float[] decoded = new float[count];
                buffer.asFloatBuffer().get(decoded);
                return decoded;
            }
            case DOUBLE: {
                double[] decoded = new double[count];
                buffer.asDoubleBuffer().get(decoded);
                return decoded;
            }
            default:
                throw new IOException("Unsupported fixed-width type: " + type);
        }
    }

    private Object decodeDictionary(BytesInput input, int count) throws IOException {
        byte[] raw = input.toByteArray();
        int bitWidth = raw[0] & 0xFF;
        int[] indices = ParquetRleDecoder.decode(bitWidth, raw, 1, count);

        switch (type) {
            case INT32: {
                int[] dict = (int[]) dictionary;
                int[] decoded = new int[count];
                for (int i = 0; i < count; i++) {
                    decoded[i] = dict[indices[i]];
                }
                return decoded;
            }
            case INT64: {
                long[] dict = (long[]) dictionary;
                long[] decoded = new long[count];
                for (int i = 0; i < count; i++) {
                    decoded[i] = dict[indices[i]];
                }
                return decoded;
            }
            case FLOAT: {
                float[] dict = (float[]) dictionary;
                float[] decoded = new float[count];
                for (int i = 0; i < count; i++) {
                    decoded[i] = dict[indices[i]];
                }
                return decoded;
            }
            case DOUBLE: {
                double[] dict = (double[]) dictionary;
                double[] decoded = new double[count];
                for (int i = 0; i < count; i++) {
                    decoded[i] = dict[indices[i]];
                }
                return decoded;
            }
            default:
                throw new IOException("Unsupported fixed-width type: " + type);
        }
    }

    private void copyToVector(FieldVector vector, int startRow, int fromIndex, int n) {
        if (type == PrimitiveType.PrimitiveTypeName.INT32) {
            int[] src = (int[]) values;
            byte[] out = new byte[n * Integer.BYTES];
            ByteBuffer.wrap(out).order(ByteOrder.LITTLE_ENDIAN).asIntBuffer().put(src, fromIndex, n);
            ((IntVector) vector).getDataBuffer().setBytes((long) startRow * Integer.BYTES, out, 0, out.length);
        } else if (type == PrimitiveType.PrimitiveTypeName.INT64) {
            long[] src = (long[]) values;
            byte[] out = new byte[n * Long.BYTES];
            ByteBuffer.wrap(out).order(ByteOrder.LITTLE_ENDIAN).asLongBuffer().put(src, fromIndex, n);
            ((BigIntVector) vector).getDataBuffer().setBytes((long) startRow * Long.BYTES, out, 0, out.length);
        } else if (type == PrimitiveType.PrimitiveTypeName.FLOAT) {
            float[] src = (float[]) values;
            byte[] out = new byte[n * Float.BYTES];
            ByteBuffer.wrap(out).order(ByteOrder.LITTLE_ENDIAN).asFloatBuffer().put(src, fromIndex, n);
            ((Float4Vector) vector).getDataBuffer().setBytes((long) startRow * Float.BYTES, out, 0, out.length);
        } else if (type == PrimitiveType.PrimitiveTypeName.DOUBLE) {
            double[] src = (double[]) values;
            byte[] out = new byte[n * Double.BYTES];
            ByteBuffer.wrap(out).order(ByteOrder.LITTLE_ENDIAN).asDoubleBuffer().put(src, fromIndex, n);
            ((Float8Vector) vector).getDataBuffer().setBytes((long) startRow * Double.BYTES, out, 0, out.length);
        } else {
            throw new IllegalStateException("Unsupported fixed-width type: " + type);
        }
    }
}
