package xsgao.lakehouseconnector.planner;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import software.amazon.awssdk.services.s3.S3Client;
import xsgao.lakehouseconnector.catalog.ColumnStats;
import xsgao.lakehouseconnector.catalog.DataFile;
import xsgao.lakehouseconnector.catalog.Field;
import xsgao.lakehouseconnector.catalog.Schema;
import xsgao.lakehouseconnector.planner.QueryPredicate.Operator;
import xsgao.lakehouseconnector.utils.S3ClientFactory;
import xsgao.lakehouseconnector.utils.S3ClientUtils;
import static xsgao.lakehouseconnector.utils.S3ClientUtils.BUCKET_NAME;
import xsgao.lakehouseconnector.writer.TableWriterService;

class TableScanPlannerTest {

    private S3Client s3Client;
    private TableWriterService writerService;
    private TableScanPlanner readerService;
    private String tableName;

    @BeforeEach
    void setUp() {
        s3Client = S3ClientFactory.createDefaultClient();
        S3ClientUtils.ensureBucketExists(s3Client, BUCKET_NAME);
        writerService = new TableWriterService(s3Client, BUCKET_NAME, 3, 50);
        readerService = new TableScanPlanner(s3Client, BUCKET_NAME);
        tableName = "table_reader_service_test_" + System.currentTimeMillis();
    }

    @AfterEach
    void tearDown() {
        if ("true".equals(System.getProperty("keepData"))) {
            return;
        }
        S3ClientUtils.deleteTable(s3Client, BUCKET_NAME, tableName);
    }

    @Test
    void testFullRoundTripWriteAndReadScanPlan() throws Exception {
                // 1. Define Table Schema and Data Files with Column Statistics
        Field idField = new Field(1, "order_id", "long", true);
        Field amountField = new Field(2, "amount", "double", false);
        Field dtField = new Field(3, "dt", "string", true);
        Schema schema = new Schema(1, List.of(idField, amountField, dtField));

        ColumnStats stats1 = new ColumnStats("0.0", "99.0", 0);
        DataFile file1 = new DataFile("data/dt=2026-11-03/part-001.parquet", 100, 1024L, Map.of("amount", stats1));

        ColumnStats stats2 = new ColumnStats("100.0", "500.0", 0);
        DataFile file2 = new DataFile("data/dt=2026-11-03/part-002.parquet", 200, 2048L, Map.of("amount", stats2));

        ColumnStats stats3 = new ColumnStats("0.0", "99.0", 0);
        DataFile file3 = new DataFile("data/dt=2026-11-04/part-003.parquet", 100, 1024L, Map.of("amount", stats3));

        ColumnStats stats4 = new ColumnStats("100.0", "500.0", 0);
        DataFile file4 = new DataFile("data/dt=2026-11-04/part-004.parquet", 200, 2048L, Map.of("amount", stats4));

        // 2. WRITE PATH: Commit transaction metadata (v1.json) to MinIO/S3
        writerService.commitTableTransaction(tableName, schema, List.of(file1, file2, file3, file4), null);

        // 3. READ PATH: Load metadata via catalog and plan scan combining partition & statistics pruning
        QueryPredicate predicate = new QueryPredicate("amount", Operator.GREATER_THAN, 300.0);
        List<TableSplit> splits = readerService.planScan(tableName, "dt=2026-11-04", List.of(predicate));

        // 4. Verify round-trip results
        assertEquals(1, splits.size(), "Round-trip plan should retain exactly 1 split after partition and stats pruning");
        assertEquals("data/dt=2026-11-04/part-004.parquet", splits.get(0).getFilePath());
    }
}