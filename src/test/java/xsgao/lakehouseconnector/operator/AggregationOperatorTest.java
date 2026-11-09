package xsgao.lakehouseconnector.operator;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import software.amazon.awssdk.services.s3.S3Client;
import xsgao.lakehouseconnector.catalog.Field;
import xsgao.lakehouseconnector.catalog.Schema;
import xsgao.lakehouseconnector.model.RecordBatch;
import xsgao.lakehouseconnector.model.Row;
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

public class AggregationOperatorTest {

    private S3Client s3Client;
    private String tableName;

    @BeforeEach
    public void setUp() {
        s3Client = S3ClientFactory.createDefaultClient();
        S3ClientUtils.ensureBucketExists(s3Client, BUCKET_NAME);
        tableName = "aggregation_operator_test_" + System.currentTimeMillis();
    }

    @AfterEach
    void tearDown() {
        if ("true".equals(System.getProperty("keepData"))) {
            return;
        }
        S3ClientUtils.deleteTable(s3Client, BUCKET_NAME, tableName);
    }

    @Test
    public void testEdgeAggregationGroupByCountry() throws IOException {
        // 1. Define schema with all 11 columns
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

        try (ParquetTableWriter writer = new ParquetTableWriter(tableSchema, path, 1024*1024, CompressionCodecName.SNAPPY, s3Client)) {
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

            // Row 2: Canada
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

        // 3. Prepare requested projection columns (needed for group by & aggregations)
        List<String> projectedColumns = Arrays.asList("country", "revenue", "tax");
        List<Field> projectedFields = tableSchema.getFields().stream()
            .filter(field -> projectedColumns.contains(field.getName()))
            .toList();

        // 4. Assemble operator chain: TableScanOperator -> AggregationOperator (No Filter)
        RowReader rowReader = new ParquetTableReader(tableSchema, projectedFields, path, s3Client);
        Iterator<RecordBatch> scanOp = new TableScanOperator(rowReader, projectedFields);

        List<String> groupByKeys = Collections.singletonList("country");
        List<AggregationOperator.AggExpr> aggregations = new ArrayList<>();
        aggregations.add(new AggregationOperator.AggExpr("revenue", AggregationOperator.AggFunction.COUNT, "count_orders"));
        aggregations.add(new AggregationOperator.AggExpr("revenue", AggregationOperator.AggFunction.SUM, "total_revenue"));
        aggregations.add(new AggregationOperator.AggExpr("tax", AggregationOperator.AggFunction.SUM, "total_tax"));

        Iterator<RecordBatch> reader = new AggregationOperator(scanOp, groupByKeys, aggregations);

        // 5. Consume and assert the aggregated pipeline output
        assertTrue(reader.hasNext(), "Reader should have aggregated batches");

        RecordBatch aggBatch = reader.next();

        // Expect 2 distinct groups: USA and Canada
        assertEquals(2, aggBatch.getRowCount(), "Should yield exactly 2 grouped rows (USA and Canada)");

        // Verify aggregated totals per country
        Map<String, Map<String, Double>> results = new HashMap<>();
        for (int i = 0; i < aggBatch.getRowCount(); i++) {
            Row row = aggBatch.getRow(i);
            String country = (String) row.getValue("country");
            double count = ((Number) row.getValue("count_orders")).doubleValue();
            double rev = ((Number) row.getValue("total_revenue")).doubleValue();
            double tax = ((Number) row.getValue("total_tax")).doubleValue();
            results.put(country, Map.of("count", count, "revenue", rev, "tax", tax));
        }

        assertTrue(results.containsKey("USA"));
        assertEquals(2.0, results.get("USA").get("count"), 0.001);
        assertEquals(195.0, results.get("USA").get("revenue"), 0.001);
        assertEquals(15.6, results.get("USA").get("tax"), 0.001);

        assertTrue(results.containsKey("Canada"));
        assertEquals(1.0, results.get("Canada").get("count"), 0.001);
        assertEquals(190.0, results.get("Canada").get("revenue"), 0.001);
        assertEquals(15.2, results.get("Canada").get("tax"), 0.001);

        assertFalse(reader.hasNext(), "No more batches should remain");
    }
}