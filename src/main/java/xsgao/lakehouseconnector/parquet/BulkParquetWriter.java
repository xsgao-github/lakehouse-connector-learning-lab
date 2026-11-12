package xsgao.lakehouseconnector.parquet;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.PageWriter;
import org.apache.parquet.column.values.dictionary.DictionaryValuesWriter;
import org.apache.parquet.column.values.dictionary.DictionaryValuesWriter.PlainBinaryDictionaryValuesWriter;
import org.apache.parquet.column.values.dictionary.DictionaryValuesWriter.PlainDoubleDictionaryValuesWriter;
import org.apache.parquet.column.values.delta.DeltaBinaryPackingValuesWriterForInteger;
import org.apache.parquet.column.values.delta.DeltaBinaryPackingValuesWriterForLong;
import org.apache.parquet.column.values.deltalengthbytearray.DeltaLengthByteArrayValuesWriter;
import org.apache.parquet.column.values.dictionary.DictionaryValuesWriter.PlainFloatDictionaryValuesWriter;
import org.apache.parquet.column.values.dictionary.DictionaryValuesWriter.PlainIntegerDictionaryValuesWriter;
import org.apache.parquet.column.values.dictionary.DictionaryValuesWriter.PlainLongDictionaryValuesWriter;
import org.apache.parquet.column.statistics.BinaryStatistics;
import org.apache.parquet.column.statistics.DoubleStatistics;
import org.apache.parquet.column.statistics.FloatStatistics;
import org.apache.parquet.column.statistics.IntStatistics;
import org.apache.parquet.column.statistics.LongStatistics;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.CodecFactory;
import org.apache.parquet.hadoop.ColumnChunkPageWriteStore;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import xsgao.lakehouseconnector.vector.VectorBatch;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Collections;
import java.util.function.Function;

/**
 * Bulk Parquet encoder: writes one PLAIN-encoded data page per column per batch straight
 * from Arrow buffers, without going through {@code RecordConsumer}.
 *
 * <p>Supported layout matches the reader's bulk path: required flat columns of type
 * INT32, INT64, FLOAT, DOUBLE or BINARY. Min/max statistics are computed while scanning
 * the vectors so files remain prunable.
 *
 * <p>Encoding is chosen per column: BINARY, INT32, FLOAT and DOUBLE use dictionary pages
 * with RLE/bit-packed indices (low-cardinality strings and measures shrink a lot); INT64
 * columns (ids/timestamps) stay PLAIN because their dictionaries would be as large as the
 * data. A dictionary that exceeds the page threshold falls back to PLAIN for that chunk.
 */
final class BulkParquetWriter implements Closeable {

    private final ParquetFileWriter fileWriter;
    private final MessageType schema;
    private final CodecFactory codecFactory;
    private final CodecFactory.BytesCompressor compressor;
    private final Function<String, ColumnEncoding> encodingStrategy;

    BulkParquetWriter(Path filePath, Configuration conf, MessageType schema) throws IOException {
        this(filePath, conf, schema, null);
    }

    BulkParquetWriter(Path filePath, Configuration conf, MessageType schema,
                      Function<String, ColumnEncoding> encodingStrategy) throws IOException {
        this.schema = schema;
        this.encodingStrategy = encodingStrategy;
        this.fileWriter = new ParquetFileWriter(conf, schema, filePath, ParquetFileWriter.Mode.CREATE);
        this.fileWriter.start();
        this.codecFactory = new CodecFactory(conf, ParquetProperties.DEFAULT_PAGE_SIZE);
        this.compressor = codecFactory.getCompressor(CompressionCodecName.SNAPPY);
    }

    void writeBatch(VectorBatch batch) throws IOException {
        int rows = batch.getRowCount();
        if (rows == 0) {
            return;
        }

        VectorSchemaRoot root = batch.getRoot();
        ColumnChunkPageWriteStore pageStore = new ColumnChunkPageWriteStore(
                compressor, schema, new HeapByteBufferAllocator(),
                ParquetProperties.DEFAULT_COLUMN_INDEX_TRUNCATE_LENGTH);

        fileWriter.startBlock(rows);
        for (int i = 0; i < schema.getFieldCount(); i++) {
            String name = schema.getFieldName(i);
            ColumnDescriptor descriptor = schema.getColumns().get(i);
            FieldVector vector = root.getVector(name);
            if (vector == null) {
                throw new IOException("Column missing from batch: " + name);
            }

            PrimitiveType.PrimitiveTypeName type = descriptor.getPrimitiveType().getPrimitiveTypeName();
            ColumnEncoding requested = encodingStrategy == null ? null : encodingStrategy.apply(name);
            ColumnEncoding encoding = requested != null ? requested : defaultEncoding(type);

            PageWriter pageWriter = pageStore.getPageWriter(descriptor);
            Statistics<?> statistics = computeStatistics(vector, rows, descriptor);
            writeColumn(pageWriter, vector, descriptor, rows, statistics, encoding);
        }
        pageStore.flushToFileWriter(fileWriter);
        fileWriter.endBlock();
    }

    private static ColumnEncoding defaultEncoding(PrimitiveType.PrimitiveTypeName type) {
        if (type == PrimitiveType.PrimitiveTypeName.INT64) {
            return ColumnEncoding.DELTA;
        }
        if (type == PrimitiveType.PrimitiveTypeName.BINARY
                || type == PrimitiveType.PrimitiveTypeName.INT32
                || type == PrimitiveType.PrimitiveTypeName.FLOAT
                || type == PrimitiveType.PrimitiveTypeName.DOUBLE) {
            return ColumnEncoding.DICTIONARY;
        }
        return ColumnEncoding.PLAIN;
    }

    private void writeColumn(PageWriter pageWriter, FieldVector vector, ColumnDescriptor descriptor,
                             int rows, Statistics<?> statistics, ColumnEncoding encoding) throws IOException {
        switch (encoding) {
            case PLAIN -> writePlain(pageWriter, vector, rows, statistics);
            case DELTA -> {
                if (writeDeltaPage(pageWriter, vector, descriptor, rows, statistics)) {
                    return;
                }
                if (writeDictionaryPage(pageWriter, vector, descriptor, rows, statistics)) {
                    return;
                }
                writePlain(pageWriter, vector, rows, statistics);
            }
            case DICTIONARY -> {
                if (writeDictionaryPage(pageWriter, vector, descriptor, rows, statistics)) {
                    return;
                }
                writePlain(pageWriter, vector, rows, statistics);
            }
        }
    }

    private void writePlain(PageWriter pageWriter, FieldVector vector, int rows,
                            Statistics<?> statistics) throws IOException {
        pageWriter.writePage(encodePlain(vector, rows), rows, rows, statistics,
                Encoding.RLE, Encoding.BIT_PACKED, Encoding.PLAIN);
    }

    /**
     * Writes a delta-encoded page: {@code DELTA_BINARY_PACKED} for INT32/INT64 and
     * {@code DELTA_LENGTH_BYTE_ARRAY} for BINARY.
     *
     * @return false when the type has no delta encoding; the caller then falls back.
     */
    private boolean writeDeltaPage(PageWriter pageWriter, FieldVector vector, ColumnDescriptor descriptor,
                                   int rows, Statistics<?> statistics) throws IOException {
        PrimitiveType.PrimitiveTypeName type = descriptor.getPrimitiveType().getPrimitiveTypeName();
        HeapByteBufferAllocator allocator = new HeapByteBufferAllocator();
        int pageSize = ParquetProperties.DEFAULT_PAGE_SIZE;

        if (type == PrimitiveType.PrimitiveTypeName.INT32 && vector instanceof IntVector ints) {
            DeltaBinaryPackingValuesWriterForInteger writer =
                    new DeltaBinaryPackingValuesWriterForInteger(pageSize, pageSize, allocator);
            ArrowBuf data = ints.getDataBuffer();
            for (int i = 0; i < rows; i++) {
                writer.writeInteger(data.getInt((long) i * Integer.BYTES));
            }
            pageWriter.writePage(writer.getBytes(), rows, rows, statistics,
                    Encoding.RLE, Encoding.BIT_PACKED, writer.getEncoding());
            writer.reset();
            return true;
        }
        if (type == PrimitiveType.PrimitiveTypeName.INT64 && vector instanceof BigIntVector longs) {
            DeltaBinaryPackingValuesWriterForLong writer =
                    new DeltaBinaryPackingValuesWriterForLong(pageSize, pageSize, allocator);
            ArrowBuf data = longs.getDataBuffer();
            for (int i = 0; i < rows; i++) {
                writer.writeLong(data.getLong((long) i * Long.BYTES));
            }
            pageWriter.writePage(writer.getBytes(), rows, rows, statistics,
                    Encoding.RLE, Encoding.BIT_PACKED, writer.getEncoding());
            writer.reset();
            return true;
        }
        if (type == PrimitiveType.PrimitiveTypeName.BINARY && vector instanceof VarCharVector strings) {
            DeltaLengthByteArrayValuesWriter writer =
                    new DeltaLengthByteArrayValuesWriter(pageSize, pageSize, allocator);
            ArrowBuf offsets = strings.getOffsetBuffer();
            ArrowBuf data = strings.getDataBuffer();
            for (int i = 0; i < rows; i++) {
                int start = offsets.getInt((long) i * Integer.BYTES);
                int length = offsets.getInt((long) (i + 1) * Integer.BYTES) - start;
                byte[] value = new byte[length];
                data.getBytes(start, value, 0, length);
                writer.writeBytes(Binary.fromConstantByteArray(value));
            }
            pageWriter.writePage(writer.getBytes(), rows, rows, statistics,
                    Encoding.RLE, Encoding.BIT_PACKED, writer.getEncoding());
            writer.reset();
            return true;
        }
        return false;
    }

    /**
     * Writes a dictionary page plus an RLE/bit-packed data page when the column type is a
     * good dictionary candidate and its dictionary fits the page threshold.
     *
     * @return true when the column was written with dictionary encoding; false means the
     *         caller must write a PLAIN page.
     */
    private boolean writeDictionaryPage(PageWriter pageWriter, FieldVector vector,
                                        ColumnDescriptor descriptor, int rows,
                                        Statistics<?> statistics) throws IOException {
        PrimitiveType.PrimitiveTypeName type = descriptor.getPrimitiveType().getPrimitiveTypeName();
        int maxDictionarySize = ParquetProperties.DEFAULT_DICTIONARY_PAGE_SIZE;
        HeapByteBufferAllocator allocator = new HeapByteBufferAllocator();

        if (type == PrimitiveType.PrimitiveTypeName.BINARY && vector instanceof VarCharVector strings) {
            PlainBinaryDictionaryValuesWriter writer = new PlainBinaryDictionaryValuesWriter(
                    maxDictionarySize, Encoding.RLE_DICTIONARY, Encoding.PLAIN, allocator);
            ArrowBuf offsets = strings.getOffsetBuffer();
            ArrowBuf data = strings.getDataBuffer();
            for (int i = 0; i < rows; i++) {
                int start = offsets.getInt((long) i * Integer.BYTES);
                int length = offsets.getInt((long) (i + 1) * Integer.BYTES) - start;
                byte[] value = new byte[length];
                data.getBytes(start, value, 0, length);
                writer.writeBytes(Binary.fromConstantByteArray(value));
            }
            return flushDictionary(pageWriter, writer, rows, statistics);
        }
        if (type == PrimitiveType.PrimitiveTypeName.INT64 && vector instanceof BigIntVector longs) {
            PlainLongDictionaryValuesWriter writer = new PlainLongDictionaryValuesWriter(
                    maxDictionarySize, Encoding.RLE_DICTIONARY, Encoding.PLAIN, allocator);
            ArrowBuf data = longs.getDataBuffer();
            for (int i = 0; i < rows; i++) {
                writer.writeLong(data.getLong((long) i * Long.BYTES));
            }
            return flushDictionary(pageWriter, writer, rows, statistics);
        }
        if (type == PrimitiveType.PrimitiveTypeName.INT32 && vector instanceof IntVector ints) {
            PlainIntegerDictionaryValuesWriter writer = new PlainIntegerDictionaryValuesWriter(
                    maxDictionarySize, Encoding.RLE_DICTIONARY, Encoding.PLAIN, allocator);
            ArrowBuf data = ints.getDataBuffer();
            for (int i = 0; i < rows; i++) {
                writer.writeInteger(data.getInt((long) i * Integer.BYTES));
            }
            return flushDictionary(pageWriter, writer, rows, statistics);
        }
        if (type == PrimitiveType.PrimitiveTypeName.FLOAT && vector instanceof Float4Vector floats) {
            PlainFloatDictionaryValuesWriter writer = new PlainFloatDictionaryValuesWriter(
                    maxDictionarySize, Encoding.RLE_DICTIONARY, Encoding.PLAIN, allocator);
            ArrowBuf data = floats.getDataBuffer();
            for (int i = 0; i < rows; i++) {
                writer.writeFloat(data.getFloat((long) i * Float.BYTES));
            }
            return flushDictionary(pageWriter, writer, rows, statistics);
        }
        if (type == PrimitiveType.PrimitiveTypeName.DOUBLE && vector instanceof Float8Vector doubles) {
            PlainDoubleDictionaryValuesWriter writer = new PlainDoubleDictionaryValuesWriter(
                    maxDictionarySize, Encoding.RLE_DICTIONARY, Encoding.PLAIN, allocator);
            ArrowBuf data = doubles.getDataBuffer();
            for (int i = 0; i < rows; i++) {
                writer.writeDouble(data.getDouble((long) i * Double.BYTES));
            }
            return flushDictionary(pageWriter, writer, rows, statistics);
        }
        return false;
    }

    private boolean flushDictionary(PageWriter pageWriter, PlainBinaryDictionaryValuesWriter writer,
                                    int rows, Statistics<?> statistics) throws IOException {
        if (writer.shouldFallBack()) {
            return false;
        }
        BytesInput data = writer.getBytes();
        DictionaryPage dictionaryPage = writer.toDictPageAndClose();
        if (dictionaryPage != null) {
            pageWriter.writeDictionaryPage(dictionaryPage);
        }
        pageWriter.writePage(data, rows, rows, statistics,
                Encoding.RLE, Encoding.BIT_PACKED, writer.getEncoding());
        return true;
    }

    private boolean flushDictionary(PageWriter pageWriter, PlainLongDictionaryValuesWriter writer,
                                    int rows, Statistics<?> statistics) throws IOException {
        if (writer.shouldFallBack()) {
            return false;
        }
        BytesInput data = writer.getBytes();
        DictionaryPage dictionaryPage = writer.toDictPageAndClose();
        if (dictionaryPage != null) {
            pageWriter.writeDictionaryPage(dictionaryPage);
        }
        pageWriter.writePage(data, rows, rows, statistics,
                Encoding.RLE, Encoding.BIT_PACKED, writer.getEncoding());
        return true;
    }

    private boolean flushDictionary(PageWriter pageWriter, PlainIntegerDictionaryValuesWriter writer,
                                    int rows, Statistics<?> statistics) throws IOException {
        if (writer.shouldFallBack()) {
            return false;
        }
        BytesInput data = writer.getBytes();
        DictionaryPage dictionaryPage = writer.toDictPageAndClose();
        if (dictionaryPage != null) {
            pageWriter.writeDictionaryPage(dictionaryPage);
        }
        pageWriter.writePage(data, rows, rows, statistics,
                Encoding.RLE, Encoding.BIT_PACKED, writer.getEncoding());
        return true;
    }

    private boolean flushDictionary(PageWriter pageWriter, PlainFloatDictionaryValuesWriter writer,
                                    int rows, Statistics<?> statistics) throws IOException {
        if (writer.shouldFallBack()) {
            return false;
        }
        BytesInput data = writer.getBytes();
        DictionaryPage dictionaryPage = writer.toDictPageAndClose();
        if (dictionaryPage != null) {
            pageWriter.writeDictionaryPage(dictionaryPage);
        }
        pageWriter.writePage(data, rows, rows, statistics,
                Encoding.RLE, Encoding.BIT_PACKED, writer.getEncoding());
        return true;
    }

    private boolean flushDictionary(PageWriter pageWriter, PlainDoubleDictionaryValuesWriter writer,
                                    int rows, Statistics<?> statistics) throws IOException {
        if (writer.shouldFallBack()) {
            return false;
        }
        BytesInput data = writer.getBytes();
        DictionaryPage dictionaryPage = writer.toDictPageAndClose();
        if (dictionaryPage != null) {
            pageWriter.writeDictionaryPage(dictionaryPage);
        }
        pageWriter.writePage(data, rows, rows, statistics,
                Encoding.RLE, Encoding.BIT_PACKED, writer.getEncoding());
        return true;
    }

    private BytesInput encodePlain(FieldVector vector, int rows) throws IOException {
        if (vector instanceof IntVector) {
            return BytesInput.from(copyBuffer(vector.getDataBuffer(), (long) rows * Integer.BYTES));
        }
        if (vector instanceof BigIntVector) {
            return BytesInput.from(copyBuffer(vector.getDataBuffer(), (long) rows * Long.BYTES));
        }
        if (vector instanceof Float4Vector) {
            return BytesInput.from(copyBuffer(vector.getDataBuffer(), (long) rows * Float.BYTES));
        }
        if (vector instanceof Float8Vector) {
            return BytesInput.from(copyBuffer(vector.getDataBuffer(), (long) rows * Double.BYTES));
        }
        if (vector instanceof VarCharVector stringVector) {
            return BytesInput.from(encodeBinary(stringVector, rows));
        }
        throw new IOException("Vector type not supported for bulk writing: " + vector.getClass().getName());
    }

    private static Statistics<?> computeStatistics(FieldVector vector, int rows, ColumnDescriptor descriptor) {
        Statistics<?> statistics = Statistics.createStats(descriptor.getPrimitiveType());

        if (vector instanceof IntVector v) {
            IntStatistics typed = (IntStatistics) statistics;
            ArrowBuf buffer = v.getDataBuffer();
            for (int i = 0; i < rows; i++) {
                typed.updateStats(buffer.getInt((long) i * Integer.BYTES));
            }
        } else if (vector instanceof BigIntVector v) {
            LongStatistics typed = (LongStatistics) statistics;
            ArrowBuf buffer = v.getDataBuffer();
            for (int i = 0; i < rows; i++) {
                typed.updateStats(buffer.getLong((long) i * Long.BYTES));
            }
        } else if (vector instanceof Float4Vector v) {
            FloatStatistics typed = (FloatStatistics) statistics;
            ArrowBuf buffer = v.getDataBuffer();
            for (int i = 0; i < rows; i++) {
                typed.updateStats(buffer.getFloat((long) i * Float.BYTES));
            }
        } else if (vector instanceof Float8Vector v) {
            DoubleStatistics typed = (DoubleStatistics) statistics;
            ArrowBuf buffer = v.getDataBuffer();
            for (int i = 0; i < rows; i++) {
                typed.updateStats(buffer.getDouble((long) i * Double.BYTES));
            }
        } else if (vector instanceof VarCharVector v) {
            BinaryStatistics typed = (BinaryStatistics) statistics;
            ArrowBuf offsets = v.getOffsetBuffer();
            ArrowBuf data = v.getDataBuffer();
            for (int i = 0; i < rows; i++) {
                int start = offsets.getInt((long) i * Integer.BYTES);
                int length = valueLength(offsets, i);
                byte[] bytes = new byte[length];
                data.getBytes(start, bytes, 0, length);
                typed.updateStats(Binary.fromConstantByteArray(bytes));
            }
        }
        return statistics;
    }

    private static byte[] copyBuffer(ArrowBuf buffer, long length) {
        byte[] out = new byte[Math.toIntExact(length)];
        buffer.getBytes(0, out, 0, out.length);
        return out;
    }

    private static byte[] encodeBinary(VarCharVector vector, int rows) {
        ArrowBuf offsets = vector.getOffsetBuffer();
        ArrowBuf data = vector.getDataBuffer();

        int totalSize = 0;
        for (int i = 0; i < rows; i++) {
            totalSize += Integer.BYTES + valueLength(offsets, i);
        }

        byte[] out = new byte[totalSize];
        ByteBuffer buffer = ByteBuffer.wrap(out).order(ByteOrder.LITTLE_ENDIAN);
        int position = 0;
        for (int i = 0; i < rows; i++) {
            int start = offsets.getInt((long) i * Integer.BYTES);
            int length = valueLength(offsets, i);
            buffer.putInt(position, length);
            position += Integer.BYTES;
            data.getBytes(start, out, position, length);
            position += length;
        }
        return out;
    }

    private static int valueLength(ArrowBuf offsets, int row) {
        int start = offsets.getInt((long) row * Integer.BYTES);
        int end = offsets.getInt((long) (row + 1) * Integer.BYTES);
        return end - start;
    }

    @Override
    public void close() throws IOException {
        try {
            fileWriter.end(Collections.emptyMap());
        } finally {
            codecFactory.release();
        }
    }
}
