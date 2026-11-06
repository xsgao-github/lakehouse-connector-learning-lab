package xsgao.lakehouseconnector.reader;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3Client;
import xsgao.lakehouseconnector.catalog.Field;
import xsgao.lakehouseconnector.catalog.Schema;
import xsgao.lakehouseconnector.model.RecordBatch;
import xsgao.lakehouseconnector.model.Row;
import xsgao.lakehouseconnector.planner.QueryPredicate;
import xsgao.lakehouseconnector.planner.TableSplit;
import xsgao.lakehouseconnector.utils.S3ClientFactory;
import static xsgao.lakehouseconnector.utils.S3ClientUtils.BUCKET_NAME;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

public class ProjectedScanReaderTest {

    private S3Client s3Client;

    @BeforeEach
    public void setUp() {
        s3Client = S3ClientFactory.createDefaultClient();
    }

    @Test
    public void testColumnarProjectionAndPredicateFiltering() {
        // 1. Define schema with all 11 columns
        List<Field> allFields = Arrays.asList(
            new Field(1, "order_id", "STRING", true),
            new Field(2, "customer_id", "STRING", true),
            new Field(3, "order_date", "STRING", true),
            new Field(4, "country", "STRING", true),
            new Field(5, "product_id", "STRING", true),
            new Field(6, "quantity", "INTEGER", true),
            new Field(7, "unit_price", "DOUBLE", true),
            new Field(8, "discount", "DOUBLE", true),
            new Field(9, "shipping_cost", "DOUBLE", true),
            new Field(10, "revenue", "DOUBLE", true),
            new Field(11, "tax", "DOUBLE", true)
        );
        Schema tableSchema = new Schema(100, allFields);

        // 2. Use a real split pointing to the Parquet file on the mock S3 server
        // The file is located at tables/module06/data/orders_part_01.parquet
        TableSplit split = new TableSplit("tables/module06/data/orders_part_01.parquet", 0L, 0L, 0L);

        // 3. Use a real TableReaderService that reads from S3/Parquet
        TableReaderService readerService = new TableReaderService(s3Client, BUCKET_NAME);

        // 4. Request exactly 3 projected columns: country, revenue, tax
        List<String> projectedColumns = Arrays.asList("country", "revenue", "tax");

        // 5. Define predicate: country = 'USA'
        QueryPredicate predicate = new QueryPredicate("country", QueryPredicate.Operator.EQUALS, "USA");

        // 6. Instantiate ProjectedScanReader
        ProjectedScanReader reader = new ProjectedScanReader(
            split,
            projectedColumns,
            predicate,
            tableSchema,
            readerService
        );

        // 7. Actually consume and assert the read path
        assertTrue(reader.hasNext(), "Reader should have matching batches");

        RecordBatch filteredBatch = reader.next();

        // Assert row filtering: check that only matching rows survived
        // Note: The exact row count depends on the data in orders_part_01.parquet
        assertTrue(filteredBatch.getRowCount() > 0, "Should have at least one matching row");

        // Assert columnar projection: fields in filteredBatch should match the 3 requested columns
        assertEquals(3, filteredBatch.getFields().size(), "Batch should be projected to exactly 3 columns");
        assertTrue(filteredBatch.getFields().stream().allMatch(f -> projectedColumns.contains(f.getName())));

        // Verify row content for the first matching row
        Row survivingRow = filteredBatch.getRow(0);
        assertEquals("USA", survivingRow.getValue("country"));

        // The loop continues until all batches are processed
        while(reader.hasNext()) {
            RecordBatch batch = reader.next();
            assertEquals(3, batch.getFields().size());
            for (Row row : batch.getRows()) {
                assertEquals("USA", row.getValue("country"));
            }
        }
    }
}