package xsgao.lakehouseconnector.operator;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3Client;
import xsgao.lakehouseconnector.catalog.Field;
import xsgao.lakehouseconnector.catalog.Schema;
import xsgao.lakehouseconnector.model.RecordBatch;
import xsgao.lakehouseconnector.model.Row;
import xsgao.lakehouseconnector.parquet.ParquetTableReader;
import xsgao.lakehouseconnector.parquet.ParquetTableWriter;
import xsgao.lakehouseconnector.reader.RowReader;
import xsgao.lakehouseconnector.utils.S3ClientFactory;
import xsgao.lakehouseconnector.utils.S3ClientUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static xsgao.lakehouseconnector.utils.S3ClientUtils.BUCKET_NAME;

public class TableScanOperatorTest {    private S3Client s3Client;    private String tableName;

    @BeforeEach
    public void setUp() {
        s3Client = S3ClientFactory.createDefaultClient();
        S3ClientUtils.ensureBucketExists(s3Client, BUCKET_NAME);
        tableName = "table_scan_projection_test_" + System.currentTimeMillis();
    }

    @AfterEach
    void tearDown() {
        if ("true".equals(System.getProperty("keepData"))) {
            return;
        }
        S3ClientUtils.deleteTable(s3Client, BUCKET_NAME, tableName);
    }

    @Test
    public void testColumnProjectionIsAppliedDuringTableScan() throws IOException {
        // 1. Define the full table schema (all optional to avoid InvalidRecordException)
        List<Field> allFields = Arrays.asList(
            new Field(1, "order_id", "STRING", false),
            new Field(2, "country", "STRING", false),
            new Field(3, "quantity", "INTEGER", false),
            new Field(4, "revenue", "DOUBLE", false),
            new Field(5, "tax", "DOUBLE", false)
        );
        Schema tableSchema = new Schema(100, allFields);

        // 2. Write sample data to S3 using ParquetTableWriter
        Path path = new Path("s3a://" + BUCKET_NAME + "/tables/" + tableName + "/data/orders_part_01.parquet");
        try (ParquetTableWriter writer = new ParquetTableWriter(tableSchema, path, 1024 * 1024, CompressionCodecName.SNAPPY)) {
            writer.writeRecord(order("ORD-1001", "USA", 2, 100.0, 8.0));
            writer.writeRecord(order("ORD-1002", "Canada", 1, 190.0, 15.2));
            writer.writeRecord(order("ORD-1003", "USA", 5, 95.0, 7.6));
        }

        // 3. Request only a subset of columns; the reader owns the projection
        List<String> projectedColumns = Arrays.asList("country", "revenue", "tax");
        List<Field> projectedFields = tableSchema.getFields().stream()
            .filter(field -> projectedColumns.contains(field.getName()))
            .toList();

        RowReader rowReader = new ParquetTableReader(tableSchema, projectedFields, path);
        Iterator<RecordBatch> scanOp = new TableScanOperator(rowReader);

        // 4. Consume the scan and assert the batch schema mirrors the projection
        assertTrue(scanOp.hasNext(), "Scan should produce at least one batch");

        RecordBatch batch = scanOp.next();
        assertEquals(3, batch.getRowCount());
        assertEquals(projectedColumns, batch.getFields().stream().map(Field::getName).toList(),
            "Batch schema should contain exactly the projected columns");

        // 5. Verify projected values and that unprojected columns are absent
        Row firstRow = batch.getRow(0);
        assertEquals("USA", firstRow.getValue("country"));
        assertEquals(100.0, firstRow.getValue("revenue"));
        assertEquals(8.0, firstRow.getValue("tax"));
        assertFalse(firstRow.getValues().containsKey("order_id"));
        assertFalse(firstRow.getValues().containsKey("quantity"));

        Row secondRow = batch.getRow(1);
        assertEquals("Canada", secondRow.getValue("country"));
        assertEquals(190.0, secondRow.getValue("revenue"));
        assertEquals(15.2, secondRow.getValue("tax"));

        Row thirdRow = batch.getRow(2);
        assertEquals("USA", thirdRow.getValue("country"));
        assertEquals(95.0, thirdRow.getValue("revenue"));
        assertEquals(7.6, thirdRow.getValue("tax"));

        assertFalse(scanOp.hasNext(), "No more batches should remain");
    }

    private static Map<String, Object> order(String orderId, String country, int quantity, double revenue, double tax) {
        Map<String, Object> row = new HashMap<>();
        row.put("order_id", orderId);
        row.put("country", country);
        row.put("quantity", quantity);
        row.put("revenue", revenue);
        row.put("tax", tax);
        return row;
    }
}
