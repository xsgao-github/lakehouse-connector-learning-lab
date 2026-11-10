package xsgao.lakehouseconnector.operator;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3Client;
import xsgao.lakehouseconnector.catalog.Field;
import xsgao.lakehouseconnector.catalog.Schema;
import xsgao.lakehouseconnector.model.RecordBatch;
import xsgao.lakehouseconnector.model.Row;
import xsgao.lakehouseconnector.planner.QueryPredicate;
import xsgao.lakehouseconnector.parquet.ParquetTableReader;
import xsgao.lakehouseconnector.reader.RowReader;
import xsgao.lakehouseconnector.utils.S3ClientFactory;
import xsgao.lakehouseconnector.utils.S3ClientUtils;
import xsgao.lakehouseconnector.parquet.ParquetTableWriter;
import static xsgao.lakehouseconnector.utils.S3ClientUtils.BUCKET_NAME;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.hadoop.fs.Path;

import java.util.*;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

public class FilterOperatorTest {    private S3Client s3Client;    private String tableName;

    @BeforeEach
    public void setUp() {
        s3Client = S3ClientFactory.createDefaultClient();
        S3ClientUtils.ensureBucketExists(s3Client, BUCKET_NAME);
        tableName = "projected_scan_reader_test_" + System.currentTimeMillis();
    }

    @AfterEach
    void tearDown() {
        if ("true".equals(System.getProperty("keepData"))) {
            return;
        }
        S3ClientUtils.deleteTable(s3Client, BUCKET_NAME, tableName);
    }

    @Test
    public void testColumnarProjectionAndPredicateFiltering() throws IOException {
        // 1. Define schema with all 11 columns (all optional to avoid InvalidRecordException)
        List<Field> allFields = Arrays.asList(
            new Field(1, "order_id", "STRING", false),
            new Field(2, "customer_id", "STRING", false),
            new Field(3, "order_date", "STRING", false),
            new Field(4, "country", "STRING", false),
            new Field(5, "product_id", "STRING", false),
            new Field(6, "quantity", "INTEGER", false),
            new Field(7, "unit_price", "DOUBLE", false),
            new Field(8, "discount", "DOUBLE", false),
            new Field(9, "shipping_cost", "DOUBLE", false),
            new Field(10, "revenue", "DOUBLE", false),
            new Field(11, "tax", "DOUBLE", false)
        );
        Schema tableSchema = new Schema(100, allFields);

        // 2. Generate sample data and write to S3 using ParquetTableWriter
        String filePath = "tables/" + tableName + "/data/orders_part_01.parquet";
        Path path = new Path("s3a://" + BUCKET_NAME + "/" + filePath);

        try (ParquetTableWriter writer = new ParquetTableWriter(tableSchema, path, 1024*1024, CompressionCodecName.SNAPPY)) {
            // Row 1: USA
            Map<String, Object> row1 = new HashMap<>();
            row1.put("order_id", "ORD-1001");
            row1.put("customer_id", "CUST-501");
            row1.put("order_date", "2026-11-01");
            row1.put("country", "USA");
            row1.put("product_id", "PROD-A");
            row1.put("quantity", 2);
            row1.put("unit_price", 50.0);
            row1.put("discount", 0.0);
            row1.put("shipping_cost", 5.0);
            row1.put("revenue", 100.0);
            row1.put("tax", 8.0);
            writer.writeRecord(row1);

            // Row 2: Canada (should be filtered)
            Map<String, Object> row2 = new HashMap<>();
            row2.put("order_id", "ORD-1002");
            row2.put("customer_id", "CUST-502");
            row2.put("order_date", "2026-11-02");
            row2.put("country", "Canada");
            row2.put("product_id", "PROD-B");
            row2.put("quantity", 1);
            row2.put("unit_price", 200.0);
            row2.put("discount", 10.0);
            row2.put("shipping_cost", 15.0);
            row2.put("revenue", 190.0);
            row2.put("tax", 15.2);
            writer.writeRecord(row2);

            // Row 3: USA
            Map<String, Object> row3 = new HashMap<>();
            row3.put("order_id", "ORD-1003");
            row3.put("customer_id", "CUST-503");
            row3.put("order_date", "2026-11-03");
            row3.put("country", "USA");
            row3.put("product_id", "PROD-C");
            row3.put("quantity", 5);
            row3.put("unit_price", 20.0);
            row3.put("discount", 5.0);
            row3.put("shipping_cost", 10.0);
            row3.put("revenue", 95.0);
            row3.put("tax", 7.6);
            writer.writeRecord(row3);
        }

        // 3. Prepare requested projection columns
        List<String> projectedColumns = Arrays.asList("country", "revenue", "tax");
        List<Field> projectedFields = tableSchema.getFields().stream()
            .filter(field -> projectedColumns.contains(field.getName()))
            .toList();
        QueryPredicate predicate = new QueryPredicate("country", QueryPredicate.Operator.EQUALS, "USA");

        // 4. Assemble the operator chain externally: TableScanOperator -> FilterOperator
        RowReader rowReader = new ParquetTableReader(tableSchema, projectedFields, path);
        Iterator<RecordBatch> scanOp = new TableScanOperator(rowReader);
        Iterator<RecordBatch> reader = new FilterOperator(scanOp, predicate);

        // 5. Consume and assert the chained execution pipeline
        assertTrue(reader.hasNext(), "Reader should have matching batches");

        RecordBatch filteredBatch = reader.next();

        // Assert row filtering: 2 USA rows should survive, Canada row should be dropped
        assertEquals(2, filteredBatch.getRowCount(), "Only rows matching country = 'USA' should survive");

        // Assert columnar projection: fields in filteredBatch should match the 3 requested columns
        assertEquals(3, filteredBatch.getFields().size(), "Batch should be projected to exactly 3 columns");
        assertTrue(filteredBatch.getFields().stream().allMatch(f -> projectedColumns.contains(f.getName())));

        // Verify row content
        Row firstRow = filteredBatch.getRow(0);
        assertEquals("USA", firstRow.getValue("country"));
        assertEquals(100.0, firstRow.getValue("revenue"));
        assertEquals(8.0, firstRow.getValue("tax"));

        Row secondRow = filteredBatch.getRow(1);
        assertEquals("USA", secondRow.getValue("country"));
        assertEquals(95.0, secondRow.getValue("revenue"));
        assertEquals(7.6, secondRow.getValue("tax"));

        assertFalse(reader.hasNext(), "No more batches should remain");
    }
}