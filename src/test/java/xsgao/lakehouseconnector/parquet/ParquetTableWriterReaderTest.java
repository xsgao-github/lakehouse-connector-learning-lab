package xsgao.lakehouseconnector.parquet;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import software.amazon.awssdk.services.s3.S3Client;
import xsgao.lakehouseconnector.catalog.ColumnStats;
import xsgao.lakehouseconnector.catalog.Field;
import xsgao.lakehouseconnector.catalog.Schema;
import xsgao.lakehouseconnector.utils.S3ClientFactory;
import xsgao.lakehouseconnector.utils.S3ClientUtils;
import static xsgao.lakehouseconnector.utils.S3ClientUtils.BUCKET_NAME;

public class ParquetTableWriterReaderTest {

    static {
        // Remove the fake Windows home dir to avoid "C:\bin" FileNotFoundException
        // Instead, we rely on fs.s3a.fast.upload=false in the writer to avoid native calls
    }

    private S3Client s3Client;
    private String tableName;

    @BeforeEach
    public void setUp() {
        // Initialize S3Client targeted at local MinIO / S3 endpoint
        s3Client = S3ClientFactory.createDefaultClient();
        S3ClientUtils.ensureBucketExists(s3Client, BUCKET_NAME);

        tableName = "parquet_table_writer_reader_test_" + System.currentTimeMillis();
    }

    @AfterEach
    void tearDown() {
        if ("true".equals(System.getProperty("keepData"))) {
            return;
        }
        S3ClientUtils.deleteTable(s3Client, BUCKET_NAME, tableName);
    }

    @Test
    public void testStreamingRoundTripAndStatsFallback() throws IOException {
        Schema schema = new Schema(1, Arrays.asList(
            new Field(1, "id", "int", true),
            new Field(2, "name", "string", false)
        ));

        Path parquetPath = new Path("s3a://" + BUCKET_NAME + "/tables/" + tableName + "/data.parquet");

        // 1. Write records
        try (ParquetTableWriter writer = new ParquetTableWriter(schema, parquetPath, 1024 * 1024, org.apache.parquet.hadoop.metadata.CompressionCodecName.SNAPPY, s3Client)) {
            Map<String, Object> r1 = new HashMap<>();
            r1.put("id", 101);
            r1.put("name", "Alice");
            writer.writeRecord(r1);

            Map<String, Object> r2 = new HashMap<>();
            r2.put("id", 102);
            r2.put("name", "Bob");
            writer.writeRecord(r2);
        }

        // 2. Read records back
        try (ParquetTableReader reader = new ParquetTableReader(schema, null, parquetPath, s3Client)) {
            Map<String, Object> rec1 = reader.readNextRecord();
            assertNotNull(rec1);
            assertEquals(101, rec1.get("id"));
            assertEquals("Alice", rec1.get("name"));

            Map<String, Object> rec2 = reader.readNextRecord();
            assertNotNull(rec2);
            assertEquals(102, rec2.get("id"));
            assertEquals("Bob", rec2.get("name"));

            assertNull(reader.readNextRecord());
        }

        // 3. Validate Footer Statistics Fallback
        try (ParquetTableReader reader = new ParquetTableReader(schema, null, parquetPath, s3Client)) {
            Map<String, ColumnStats> statsMap = reader.extractFooterStatistics(parquetPath);
            assertNotNull(statsMap);
            assertTrue(statsMap.containsKey("id"));

            ColumnStats idStats = statsMap.get("id");
            assertEquals("101", String.valueOf(idStats.getLowerBound()));
            assertEquals("102", String.valueOf(idStats.getUpperBound()));
            assertEquals(0L, idStats.getNullCount());
        }
    }
}