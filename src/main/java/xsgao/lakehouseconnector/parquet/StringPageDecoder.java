package xsgao.lakehouseconnector.parquet;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.DataPageV2;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.PageReader;

import java.io.IOException;

/**
 * Bulk decoder for required flat BINARY (UTF-8) columns encoded with PLAIN or
 * dictionary pages.
 *
 * <p>PLAIN pages store each value as a 4-byte little-endian length plus bytes, so a page
 * is parsed once into {@code offsets + data} arrays; dictionary pages are parsed the same
 * way once, and each data page only expands its RLE/bit-packed indices.
 */
final class StringPageDecoder implements ColumnDecoder {

    private final ColumnDescriptor descriptor;
    private final PageReader pageReader;

    private byte[] dictionaryData;
    private int[] dictionaryOffsets;

    private byte[] valuesData;
    private int[] valuesOffsets;
    private int[] dictionaryIndices;
    private int valueIndex;
    private int valueCount;

    StringPageDecoder(ColumnDescriptor descriptor, PageReader pageReader) throws IOException {
        this.descriptor = descriptor;
        this.pageReader = pageReader;

        DictionaryPage dictionaryPage = pageReader.readDictionaryPage();
        if (dictionaryPage != null) {
            byte[] raw = dictionaryPage.getBytes().toByteArray();
            int size = dictionaryPage.getDictionarySize();
            dictionaryOffsets = new int[size + 1];
            dictionaryData = copyPlainBinary(raw, size, dictionaryOffsets);
        }
    }

    @Override
    public void readRows(FieldVector vector, int startRow, int rows) throws IOException {
        VarCharVector stringVector = (VarCharVector) vector;
        int out = startRow;
        int remaining = rows;

        while (remaining > 0) {
            if (valueIndex >= valueCount) {
                loadNextPage();
            }
            int n = Math.min(remaining, valueCount - valueIndex);
            for (int i = 0; i < n; i++) {
                int value = valueIndex + i;
                int row = out + i;
                if (dictionaryIndices != null) {
                    int dictIndex = dictionaryIndices[value];
                    stringVector.setSafe(row, dictionaryData,
                            dictionaryOffsets[dictIndex],
                            dictionaryOffsets[dictIndex + 1] - dictionaryOffsets[dictIndex]);
                } else {
                    stringVector.setSafe(row, valuesData,
                            valuesOffsets[value], valuesOffsets[value + 1] - valuesOffsets[value]);
                }
            }
            valueIndex += n;
            out += n;
            remaining -= n;
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
            // Required flat column: page bytes are exactly the length-prefixed values.
            bytes = v1.getBytes();
        } else if (page instanceof DataPageV2 v2) {
            encoding = v2.getDataEncoding();
            bytes = v2.getData();
        } else {
            throw new IOException("Unsupported page type: " + page.getClass().getName());
        }

        valueCount = page.getValueCount();
        valueIndex = 0;
        dictionaryIndices = null;

        if (encoding == Encoding.PLAIN) {
            byte[] raw = bytes.toByteArray();
            valuesOffsets = new int[valueCount + 1];
            valuesData = copyPlainBinary(raw, valueCount, valuesOffsets);
        } else if (encoding == Encoding.PLAIN_DICTIONARY || encoding == Encoding.RLE_DICTIONARY) {
            if (dictionaryData == null) {
                throw new IOException("Dictionary-encoded page without dictionary: "
                        + String.join(".", descriptor.getPath()));
            }
            dictionaryIndices = decodeIndices(bytes, valueCount);
        } else {
            throw new IOException("Unsupported encoding " + encoding + " for column "
                    + String.join(".", descriptor.getPath()));
        }
    }

    private static byte[] copyPlainBinary(byte[] raw, int count, int[] offsets) throws IOException {
        int dataLength = 0;
        int position = 0;
        for (int i = 0; i < count; i++) {
            int length = readIntLittleEndian(raw, position);
            position += Integer.BYTES;
            offsets[i] = dataLength;
            dataLength += length;
            position += length;
        }
        offsets[count] = dataLength;
        if (position > raw.length) {
            throw new IOException("Malformed PLAIN BINARY page: value lengths exceed page bytes");
        }

        byte[] data = new byte[dataLength];
        position = 0;
        int out = 0;
        for (int i = 0; i < count; i++) {
            int length = readIntLittleEndian(raw, position);
            position += Integer.BYTES;
            System.arraycopy(raw, position, data, out, length);
            position += length;
            out += length;
        }
        return data;
    }

    private static int[] decodeIndices(BytesInput input, int count) throws IOException {
        byte[] raw = input.toByteArray();
        int bitWidth = raw[0] & 0xFF;
        return ParquetRleDecoder.decode(bitWidth, raw, 1, count);
    }

    private static int readIntLittleEndian(byte[] raw, int position) throws IOException {
        if (position + Integer.BYTES > raw.length) {
            throw new IOException("Malformed PLAIN BINARY page: truncated length prefix");
        }
        return (raw[position] & 0xFF)
                | (raw[position + 1] & 0xFF) << 8
                | (raw[position + 2] & 0xFF) << 16
                | (raw[position + 3] & 0xFF) << 24;
    }
}
