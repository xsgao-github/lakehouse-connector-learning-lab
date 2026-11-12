package xsgao.lakehouseconnector.parquet;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.HadoopReadOptions;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.impl.ColumnReaderImpl;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import xsgao.lakehouseconnector.vector.VectorBatch;

import java.io.Closeable;
import java.io.IOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Reads Parquet into off-heap Arrow vectors.
 *
 * <p>Required flat fixed-width columns encoded with PLAIN or dictionary pages use
 * {@link FixedWidthPageDecoder}, and required flat BINARY/UTF-8 columns use
 * {@link StringPageDecoder}; both decode whole pages and copy values into Arrow buffers
 * in bulk. Everything else (optional columns, DELTA/BYTE_STREAM_SPLIT encodings) falls
 * back to the per-value {@link ColumnReaderDecoder}.
 */
public class VectorizedParquetTableReader implements Closeable {

    private static final Logger LOGGER = Logger.getLogger(VectorizedParquetTableReader.class.getName());

    // ColumnReaderBase requires a converter, but primitive decoding reads values directly.
    private static final PrimitiveConverter UNUSED_CONVERTER = new PrimitiveConverter() {};

    private static final Set<Encoding> BULK_ENCODINGS = EnumSet.of(
            Encoding.PLAIN, Encoding.PLAIN_DICTIONARY, Encoding.RLE_DICTIONARY,
            Encoding.RLE, Encoding.BIT_PACKED);

    private final ParquetFileReader reader;
    private final MessageType parquetSchema;
    private final Schema arrowSchema;
    private final List<BlockMetaData> rowGroups;
    private final Map<String, ColumnDecoder> columnDecoders = new HashMap<>();

    private PageReadStore currentRowGroup;
    private int nextRowGroupIndex;
    private long rowIndexInGroup;
    private boolean firstBatchLogged;

    public VectorizedParquetTableReader(Path filePath, Schema arrowSchema) throws IOException {
        Configuration conf = S3AConfig.create();
        ParquetReadOptions options = HadoopReadOptions.builder(conf).build();
        this.reader = ParquetFileReader.open(HadoopInputFile.fromPath(filePath, conf), options);
        this.parquetSchema = reader.getFooter().getFileMetaData().getSchema();
        this.arrowSchema = arrowSchema;
        this.rowGroups = reader.getRowGroups();
    }

    public Schema getSchema() {
        return arrowSchema;
    }

    /**
     * Number of columns opened for the current row group. Columns absent from the reader's
     * Arrow schema are never decoded, which is how projection pushdown works.
     */
    int decodedColumnCount() {
        return columnDecoders.size();
    }

    /**
     * Whether the column is decoded by the bulk page path rather than the per-value
     * fallback. Available after the first {@link #nextBatch} call initialized the group.
     */
    boolean isBulkDecoding(String columnName) {
        ColumnDecoder decoder = columnDecoders.get(columnName);
        return decoder instanceof FixedWidthPageDecoder || decoder instanceof StringPageDecoder;
    }

    /**
     * Reads the next batch of up to {@code batchSize} rows into off-heap memory.
     *
     * @return a populated batch, or null at end of file.
     */
    public VectorBatch nextBatch(BufferAllocator allocator, int batchSize) throws IOException {
        // Advance to a row group that still has unread rows.
        while (true) {
            if (currentRowGroup == null) {
                currentRowGroup = reader.readNextRowGroup();
                if (currentRowGroup == null) {
                    return null;
                }
                if (nextRowGroupIndex >= rowGroups.size()) {
                    throw new IOException("Missing metadata for row group " + nextRowGroupIndex);
                }
                BlockMetaData block = rowGroups.get(nextRowGroupIndex++);
                rowIndexInGroup = 0;
                initColumnDecoders(block);
            }
            if (rowIndexInGroup < currentRowGroup.getRowCount()) {
                break;
            }
            currentRowGroup = null;
        }

        long remaining = currentRowGroup.getRowCount() - rowIndexInGroup;
        int rowsToRead = (int) Math.min(batchSize, remaining);

        VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, allocator);
        for (FieldVector vector : root.getFieldVectors()) {
            vector.setInitialCapacity(rowsToRead);
        }
        root.allocateNew();

        for (int i = 0; i < arrowSchema.getFields().size(); i++) {
            String column = arrowSchema.getFields().get(i).getName();
            ColumnDecoder decoder = columnDecoders.get(column);
            FieldVector vector = root.getVector(i);
            if (decoder == null) {
                // Column absent from this file (additive schema evolution): expose every row as
                // null instead of leaving the vector with value count 0.
                for (int row = 0; row < rowsToRead; row++) {
                    vector.setNull(row);
                }
                vector.setValueCount(rowsToRead);
                continue;
            }
            decoder.readRows(vector, 0, rowsToRead);
            vector.setValueCount(rowsToRead);
        }

        rowIndexInGroup += rowsToRead;
        root.setRowCount(rowsToRead);

        if (!firstBatchLogged) {
            firstBatchLogged = true;
            LOGGER.info(String.format(
                    "**** [VECTORIZED PARQUET READ] first off-heap batch: rows=%,d allocated=%,d bytes, peak=%,d bytes *****",
                    rowsToRead, allocator.getAllocatedMemory(), allocator.getPeakMemoryAllocation()));
        } else if (LOGGER.isLoggable(Level.FINE)) {
            LOGGER.fine(String.format(
                    "vector batch rows=%,d offHeapAllocated=%,d bytes offHeapPeak=%,d bytes",
                    rowsToRead, allocator.getAllocatedMemory(), allocator.getPeakMemoryAllocation()));
        }
        return new VectorBatch(root);
    }

    private void initColumnDecoders(BlockMetaData block) throws IOException {
        Map<String, ColumnChunkMetaData> chunks = new HashMap<>();
        for (ColumnChunkMetaData chunk : block.getColumns()) {
            chunks.put(chunk.getPath().toDotString(), chunk);
        }

        columnDecoders.clear();
        for (org.apache.arrow.vector.types.pojo.Field field : arrowSchema.getFields()) {
            String name = field.getName();
            if (!parquetSchema.containsField(name)) {
                // Column absent from this file (additive schema evolution); nextBatch fills it
                // with nulls.
                continue;
            }
            ColumnDescriptor descriptor = parquetSchema.getColumnDescription(new String[]{name});
            if (descriptor == null) {
                continue;
            }

            PageReader pageReader = currentRowGroup.getPageReader(descriptor);
            ColumnChunkMetaData chunk = chunks.get(String.join(".", descriptor.getPath()));
            PrimitiveType.PrimitiveTypeName parquetType = descriptor.getPrimitiveType().getPrimitiveTypeName();
            PrimitiveType.PrimitiveTypeName expected = arrowPrimitive(field);

            boolean flatRequired = descriptor.getMaxDefinitionLevel() == 0
                    && descriptor.getMaxRepetitionLevel() == 0
                    && chunk != null
                    && BULK_ENCODINGS.containsAll(chunk.getEncodings());

            ColumnDecoder decoder;
            if (flatRequired && expected != null && expected == parquetType) {
                decoder = new FixedWidthPageDecoder(descriptor, pageReader);
            } else if (flatRequired
                    && parquetType == PrimitiveType.PrimitiveTypeName.BINARY
                    && field.getType() instanceof ArrowType.Utf8) {
                decoder = new StringPageDecoder(descriptor, pageReader);
            } else {
                decoder = new ColumnReaderDecoder(descriptor,
                        new ColumnReaderImpl(descriptor, pageReader, UNUSED_CONVERTER, null));
            }
            columnDecoders.put(name, decoder);
        }

        int bulk = 0;
        for (ColumnDecoder decoder : columnDecoders.values()) {
            if (decoder instanceof FixedWidthPageDecoder || decoder instanceof StringPageDecoder) {
                bulk++;
            }
        }
        LOGGER.info(String.format(
                "**** [VECTORIZED PARQUET READ] row group %d: decoded %d of %d file columns "
                        + "(projection pushdown), bulk=%d fallback=%d, Arrow off-heap batches *****",
                nextRowGroupIndex - 1, columnDecoders.size(), parquetSchema.getFieldCount(),
                bulk, columnDecoders.size() - bulk));
    }

    private static PrimitiveType.PrimitiveTypeName arrowPrimitive(
            org.apache.arrow.vector.types.pojo.Field field) {
        ArrowType type = field.getType();
        if (type instanceof ArrowType.Int integer) {
            if (integer.getBitWidth() == 32) {
                return PrimitiveType.PrimitiveTypeName.INT32;
            }
            if (integer.getBitWidth() == 64) {
                return PrimitiveType.PrimitiveTypeName.INT64;
            }
        }
        if (type instanceof ArrowType.FloatingPoint floating) {
            return floating.getPrecision() == FloatingPointPrecision.SINGLE
                    ? PrimitiveType.PrimitiveTypeName.FLOAT
                    : PrimitiveType.PrimitiveTypeName.DOUBLE;
        }
        return null;
    }

    @Override
    public void close() throws IOException {
        if (reader != null) {
            reader.close();
        }
    }
}
