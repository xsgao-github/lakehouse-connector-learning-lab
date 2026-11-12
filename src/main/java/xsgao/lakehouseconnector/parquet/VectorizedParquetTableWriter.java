package xsgao.lakehouseconnector.parquet;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import xsgao.lakehouseconnector.vector.VectorBatch;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.function.Function;

/**
 * Writes Arrow {@link VectorBatch}es to Parquet through a {@link WriteSupport}.
 *
 * <p>Parquet's writer owns page encoding, compression, statistics and row-group
 * flushing; this class translates each Arrow vector value into Parquet's
 * {@link RecordConsumer} API.
 *
 * <p>Required flat INT32/INT64/FLOAT/DOUBLE/BINARY schemas use {@link BulkParquetWriter},
 * which encodes whole pages straight from Arrow buffers with a per-column choice of
 * PLAIN, dictionary or delta encoding. Anything else falls back to the row-at-a-time
 * {@code RecordConsumer} path below.
 *
 * <p>The fallback counts one record per {@code ParquetWriter.write(...)} call, so a batch
 * is fed row by row as a {@link RowSlice}. Emitting several messages inside a single call
 * makes the footer's row count disagree with the data.
 */
public class VectorizedParquetTableWriter implements Closeable {

    private static final int ROW_GROUP_SIZE = 128 * 1024 * 1024;

    private final BulkParquetWriter bulkWriter;
    private final ParquetWriter<RowSlice> writer;

    public VectorizedParquetTableWriter(Path filePath, MessageType parquetSchema) throws IOException {
        this(filePath, parquetSchema, null);
    }

    /**
     * @param encodingStrategy per-column encoding choice by column name; null means the
     *                         default (DELTA for INT64, DICTIONARY for BINARY/INT32/FLOAT/
     *                         DOUBLE). Ignored by the row-at-a-time fallback path.
     */
    public VectorizedParquetTableWriter(Path filePath, MessageType parquetSchema,
                                        Function<String, ColumnEncoding> encodingStrategy) throws IOException {
        Configuration conf = S3AConfig.create();
        if (supportsBulk(parquetSchema)) {
            this.bulkWriter = new BulkParquetWriter(filePath, conf, parquetSchema, encodingStrategy);
            this.writer = null;
        } else {
            this.bulkWriter = null;
            this.writer = new ParquetWriter<>(
                    filePath,
                    new RowSliceWriteSupport(parquetSchema),
                    CompressionCodecName.SNAPPY,
                    ROW_GROUP_SIZE,
                    ParquetProperties.DEFAULT_PAGE_SIZE,
                    ParquetProperties.DEFAULT_DICTIONARY_PAGE_SIZE,
                    true,
                    false,
                    ParquetProperties.WriterVersion.PARQUET_1_0,
                    conf);
        }
    }

    /**
     * Appends one off-heap batch as a row group (bulk path) or as individual records.
     */
    public void writeBatch(VectorBatch batch) throws IOException {
        if (bulkWriter != null) {
            bulkWriter.writeBatch(batch);
            return;
        }
        for (int row = 0; row < batch.getRowCount(); row++) {
            writer.write(new RowSlice(batch.getRoot(), row));
        }
    }

    boolean isBulkWriting() {
        return bulkWriter != null;
    }

    @Override
    public void close() throws IOException {
        if (bulkWriter != null) {
            bulkWriter.close();
        } else {
            writer.close();
        }
    }

    private static boolean supportsBulk(MessageType schema) {
        for (Type field : schema.getFields()) {
            if (!field.isRepetition(Type.Repetition.REQUIRED)) {
                return false;
            }
            PrimitiveType.PrimitiveTypeName type = field.asPrimitiveType().getPrimitiveTypeName();
            if (type != PrimitiveType.PrimitiveTypeName.INT32
                    && type != PrimitiveType.PrimitiveTypeName.INT64
                    && type != PrimitiveType.PrimitiveTypeName.FLOAT
                    && type != PrimitiveType.PrimitiveTypeName.DOUBLE
                    && type != PrimitiveType.PrimitiveTypeName.BINARY) {
                return false;
            }
        }
        return true;
    }

    private record RowSlice(VectorSchemaRoot root, int row) {
    }

    private static final class RowSliceWriteSupport extends WriteSupport<RowSlice> {
        private final MessageType schema;
        private RecordConsumer consumer;

        private RowSliceWriteSupport(MessageType schema) {
            this.schema = schema;
        }

        @Override
        public WriteContext init(Configuration configuration) {
            return new WriteContext(schema, Collections.emptyMap());
        }

        @Override
        public void prepareForWrite(RecordConsumer recordConsumer) {
            this.consumer = recordConsumer;
        }

        @Override
        public void write(RowSlice slice) {
            consumer.startMessage();
            for (int i = 0; i < schema.getFieldCount(); i++) {
                String name = schema.getFieldName(i);
                FieldVector vector = slice.root().getVector(name);
                if (vector == null) {
                    continue;
                }
                consumer.startField(name, i);
                if (!vector.isNull(slice.row())) {
                    writeValue(vector, slice.row());
                }
                consumer.endField(name, i);
            }
            consumer.endMessage();
        }

        private void writeValue(FieldVector vector, int row) {
            if (vector instanceof IntVector v) {
                consumer.addInteger(v.get(row));
            } else if (vector instanceof BigIntVector v) {
                consumer.addLong(v.get(row));
            } else if (vector instanceof Float4Vector v) {
                consumer.addFloat(v.get(row));
            } else if (vector instanceof Float8Vector v) {
                consumer.addDouble(v.get(row));
            } else if (vector instanceof BitVector v) {
                consumer.addBoolean(v.get(row) != 0);
            } else if (vector instanceof VarCharVector v) {
                consumer.addBinary(Binary.fromConstantByteArray(v.get(row)));
            } else {
                throw new UnsupportedOperationException(
                        "Vector type not supported for vectorized writing: " + vector.getClass().getName());
            }
        }
    }
}
