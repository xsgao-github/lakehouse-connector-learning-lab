package xsgao.lakehouseconnector.writer;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import xsgao.lakehouseconnector.catalog.CatalogService;
import xsgao.lakehouseconnector.catalog.DataFile;
import xsgao.lakehouseconnector.catalog.Field;
import xsgao.lakehouseconnector.catalog.Schema;
import xsgao.lakehouseconnector.catalog.TableMetadata;
import xsgao.lakehouseconnector.utils.S3ClientFactory;
import xsgao.lakehouseconnector.utils.S3ClientUtils;
import static xsgao.lakehouseconnector.utils.S3ClientUtils.BUCKET_NAME;

class TableWriterServiceTest {

    private S3Client s3Client;
    private TableWriterService writerService;
    private CatalogService catalogService;
    private String tableName;
    private String tableRoot;

    @BeforeEach
    void setUp() {
        s3Client = S3ClientFactory.createDefaultClient();
        S3ClientUtils.ensureBucketExists(s3Client, BUCKET_NAME);
        // Configure with 3 max retries and 50ms backoff
        writerService = new TableWriterService(s3Client, BUCKET_NAME, 3, 50);
        catalogService = new CatalogService(s3Client, BUCKET_NAME);

        tableName = "table_writer_service_test_" + System.currentTimeMillis();
        tableRoot = "tables/" + tableName;
    }

    @AfterEach
    void tearDown() {
        if ("true".equals(System.getProperty("keepData"))) {
            return;
        }
        S3ClientUtils.deleteTable(s3Client, BUCKET_NAME, tableName);
    }

    @Test
    void testOCCConflictAndSuccessfulRetryToV3WithDataFiles() throws Exception {
        // --- Step 1: Write v1 metadata and initial data file ---
        Field idField = new Field(1, "order_id", "long", true);
        Field amountField = new Field(2, "amount", "double", false);
        Field dtField = new Field(3, "dt", "string", true);
        Schema schema = new Schema(1, List.of(idField, amountField, dtField));

        String v1DataPath = "data/dt=2026-11-03/part-001.parquet";
        DataFile fileV1 = new DataFile(v1DataPath, 1500, 45021L);

        // Commit initial v1 transaction and write physical data file
        int v1Version = writerService.commitTableTransaction(tableName, schema, List.of(fileV1), null);
        assertEquals(1, v1Version, "Initial commit should be version 1");
        
        s3Client.putObject(
            b -> b.bucket(BUCKET_NAME).key(tableRoot + "/" + v1DataPath),
            RequestBody.fromBytes("101,50.00,2026-11-03".getBytes(StandardCharsets.UTF_8))
        );

        TableMetadata metadataV1 = catalogService.loadTableMetadata(tableName);
        long parentSnapshotId = metadataV1.getCurrentSnapshotId();

        // --- Step 2: Simulate two concurrent writers attempting to commit v2 simultaneously ---
        ExecutorService executor = Executors.newFixedThreadPool(2);
        AtomicInteger successfulCommits = new AtomicInteger(0);

        // Writer A (The 1st write)
        Future<Integer> writeA = executor.submit(() -> {
            DataFile fileA = new DataFile("data/dt=2026-11-04/part-writer-A.parquet", 500, 10000L);
            s3Client.putObject(
                b -> b.bucket(BUCKET_NAME).key(tableRoot + "/data/dt=2026-11-04/part-writer-A.parquet"),
                RequestBody.fromBytes("102,150.00,2026-11-04".getBytes(StandardCharsets.UTF_8))
            );
            return writerService.commitTableTransaction(tableName, schema, List.of(fileA), parentSnapshotId);
        });

        // Writer B (The 2nd write - will experience conflict on v2 attempt, fail first, then auto-bump and succeed as v3)
        Future<Integer> writeB = executor.submit(() -> {
            // Slight artificial delay to ensure Writer A claims v2 first
            Thread.sleep(30);
            DataFile fileB = new DataFile("data/dt=2026-11-04/part-writer-B.parquet", 300, 6000L);
            s3Client.putObject(
                b -> b.bucket(BUCKET_NAME).key(tableRoot + "/data/dt=2026-11-04/part-writer-B.parquet"),
                RequestBody.fromBytes("103,200.00,2026-11-04".getBytes(StandardCharsets.UTF_8))
            );
            return writerService.commitTableTransaction(tableName, schema, List.of(fileB), parentSnapshotId);
        });

        executor.shutdown();
        assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));

        int versionA = writeA.get();
        int versionB = writeB.get();

        // --- Step 3: Verify version progression (v2 and v3) ---
        assertEquals(2, versionA, "First concurrent writer should successfully commit as v2");
        assertEquals(3, versionB, "Second concurrent writer should fail v2 attempt, retry, and successfully commit as v3");

        // --- Step 4: Verify all metadata versions and data files exist in S3 ---
        assertTrue(objectExists(tableRoot + "/metadata/v1.json"), "v1 metadata must exist");
        assertTrue(objectExists(tableRoot + "/metadata/v2.json"), "v2 metadata must exist");
        assertTrue(objectExists(tableRoot + "/metadata/v3.json"), "v3 metadata must exist");
        
        assertTrue(objectExists(tableRoot + "/" + v1DataPath), "v1 data file must exist");
        assertTrue(objectExists(tableRoot + "/data/dt=2026-11-04/part-writer-A.parquet"), "Writer A data file must exist");
        assertTrue(objectExists(tableRoot + "/data/dt=2026-11-04/part-writer-B.parquet"), "Writer B data file must exist");
    }

    private boolean objectExists(String key) {
        try {
            s3Client.headObject(b -> b.bucket(BUCKET_NAME).key(key));
            return true;
        } catch (Exception e) {
            return false;
        }
    }
}