package xsgao.lakehouseconnector.parquet;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3Client;
import xsgao.lakehouseconnector.operator.VectorizedTableScanOperator;
import xsgao.lakehouseconnector.utils.S3ClientFactory;
import xsgao.lakehouseconnector.utils.S3ClientUtils;
import xsgao.lakehouseconnector.vector.VectorBatch;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static xsgao.lakehouseconnector.utils.S3ClientUtils.BUCKET_NAME;

/**
 * End-to-end vectorized Parquet write/read tests over MinIO:
 * vector write → vector read, vector write → row read, row write → vector read.
 */
public class VectorizedParquetTableWriterReaderTest {

    private static final int ROW_COUNT = 5;
    private static final String[] COUNTRIES = {"USA", "Canada", "USA", "Mexico", "USA"};
    private static final double[] REVENUES = {100.0, 190.0, 95.0, 50.0, 300.0};

    private static final String PARQUET_SCHEMA = "message VectorizedRoundTrip {"
            + " required int32 id; required binary country (STRING); required double revenue; }";

    private BufferAllocator allocator;    private S3Client s3Client;
    private String tableName;
    private Path parquetPath;
    @BeforeEach
    public void setUp() {
        allocator = new RootAllocator();
        s3Client = S3ClientFactory.createDefaultClient();
        S3ClientUtils.ensureBucketExists(s3Client, BUCKET_NAME);

        tableName = "vectorized_parquet_table_writer_reader_test_" + System.nanoTime();
        parquetPath = new Path("s3a://" + BUCKET_NAME + "/tables/" + tableName + "/data.parquet");
            }

    @AfterEach
    public void tearDown() {
        if (allocator != null) {
            allocator.close();
        }
        if ("true".equals(System.getProperty("keepData"))) {
            return;
        }
        S3ClientUtils.deleteTable(s3Client, BUCKET_NAME, tableName);
    }

    @Test
    public void testVectorizedWriteReadRoundTrip() throws IOException {
        writeVectorRows();

        try (VectorizedTableScanOperator scan = new VectorizedTableScanOperator(
                new VectorizedParquetTableReader(parquetPath, arrowSchema()), ROW_COUNT)) {
            try (VectorBatch batch = scan.nextBatch(allocator)) {
                assertThat(batch).isNotNull();
                assertThat(batch.getRowCount()).isEqualTo(ROW_COUNT);
                assertVectorValues(batch);
            }
        }
    }

    @Test
    public void testVectorizedWriteReadByRowReader() throws IOException {
        writeVectorRows();

        try (ParquetTableReader reader = new ParquetTableReader(
                rowSchema(), null, parquetPath)) {
            for (int row = 0; row < ROW_COUNT; row++) {
                Map<String, Object> record = reader.readNextRecord();
                assertThat(record).isNotNull();
                assertThat(record.get("id")).isEqualTo(1 + row);
                assertThat(record.get("country")).isEqualTo(COUNTRIES[row]);
                assertThat(record.get("revenue")).isEqualTo(REVENUES[row]);
            }
            assertThat(reader.readNextRecord()).isNull();
        }
    }

    @Test
    public void testRowWriteReadByVectorReader() throws IOException {
        writeRowRows();

        try (VectorizedTableScanOperator scan = new VectorizedTableScanOperator(
                new VectorizedParquetTableReader(parquetPath, arrowSchema()), ROW_COUNT)) {
            try (VectorBatch batch = scan.nextBatch(allocator)) {
                assertThat(batch).isNotNull();
                assertThat(batch.getRowCount()).isEqualTo(ROW_COUNT);
                assertVectorValues(batch);
            }
        }
    }

    @Test
    public void testVectorReaderReturnsNullsForColumnMissingFromFile() throws IOException {
        writeVectorRows();

        try (VectorizedTableScanOperator scan = new VectorizedTableScanOperator(
                new VectorizedParquetTableReader(parquetPath, arrowSchemaWithRegion()), ROW_COUNT)) {
            try (VectorBatch batch = scan.nextBatch(allocator)) {
                assertThat(batch).isNotNull();
                assertThat(batch.getRowCount()).isEqualTo(ROW_COUNT);
                assertVectorValues(batch);

                // The file predates the nullable "region" column; every row must read as null
                // rather than a zero-length vector.
                VarCharVector regions = (VarCharVector) batch.getRoot().getVector("region");
                assertThat(regions.getValueCount()).isEqualTo(ROW_COUNT);
                for (int row = 0; row < ROW_COUNT; row++) {
                    assertThat(regions.isNull(row)).isTrue();
                }
            }
        }
    }

    private void assertVectorValues(VectorBatch batch) {
        IntVector ids = (IntVector) batch.getRoot().getVector("id");
        VarCharVector countries = (VarCharVector) batch.getRoot().getVector("country");
        Float8Vector revenues = (Float8Vector) batch.getRoot().getVector("revenue");
        for (int row = 0; row < ROW_COUNT; row++) {
            assertThat(ids.get(row)).isEqualTo(1 + row);
            assertThat(new String(countries.get(row), StandardCharsets.UTF_8)).isEqualTo(COUNTRIES[row]);
            assertThat(revenues.get(row)).isEqualTo(REVENUES[row]);
        }
    }

    private void writeVectorRows() throws IOException {
        MessageType parquetMessageType = MessageTypeParser.parseMessageType(PARQUET_SCHEMA);

        try (VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema(), allocator)) {
            root.allocateNew();
            IntVector ids = (IntVector) root.getVector("id");
            VarCharVector countries = (VarCharVector) root.getVector("country");
            Float8Vector revenues = (Float8Vector) root.getVector("revenue");
            for (int row = 0; row < ROW_COUNT; row++) {
                ids.setSafe(row, 1 + row);
                countries.setSafe(row, COUNTRIES[row].getBytes(StandardCharsets.UTF_8));
                revenues.setSafe(row, REVENUES[row]);
            }
            root.setRowCount(ROW_COUNT);

            try (VectorizedParquetTableWriter writer = new VectorizedParquetTableWriter(parquetPath, parquetMessageType)) {
                writer.writeBatch(new VectorBatch(root));
            }
        }
    }

    private void writeRowRows() throws IOException {
        try (ParquetTableWriter writer = new ParquetTableWriter(
                rowSchema(), parquetPath, 1024 * 1024, CompressionCodecName.SNAPPY)) {
            for (int row = 0; row < ROW_COUNT; row++) {
                Map<String, Object> record = new HashMap<>();
                record.put("id", 1 + row);
                record.put("country", COUNTRIES[row]);
                record.put("revenue", REVENUES[row]);
                writer.writeRecord(record);
            }
        }
    }

    private Schema arrowSchema() {
        return new Schema(List.of(
                Field.notNullable("id", new ArrowType.Int(32, true)),
                Field.notNullable("country", new ArrowType.Utf8()),
                Field.notNullable("revenue", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE))));
    }

    private Schema arrowSchemaWithRegion() {
        return new Schema(List.of(
                Field.notNullable("id", new ArrowType.Int(32, true)),
                Field.notNullable("country", new ArrowType.Utf8()),
                Field.notNullable("revenue", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
                Field.nullable("region", new ArrowType.Utf8())));
    }

    private xsgao.lakehouseconnector.catalog.Schema rowSchema() {
        return new xsgao.lakehouseconnector.catalog.Schema(1, List.of(
                new xsgao.lakehouseconnector.catalog.Field(1, "id", "int", true),
                new xsgao.lakehouseconnector.catalog.Field(2, "country", "string", true),
                new xsgao.lakehouseconnector.catalog.Field(3, "revenue", "double", true)));
    }
}
