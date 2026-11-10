package xsgao.lakehouseconnector.catalog;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.AfterEach;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import xsgao.lakehouseconnector.utils.S3ClientFactory;
import xsgao.lakehouseconnector.utils.S3ClientUtils;
import static xsgao.lakehouseconnector.utils.S3ClientUtils.BUCKET_NAME;

public class CatalogServiceTest {

    private S3Client s3Client;
    private CatalogService catalogService;
    private String tableName;

    @BeforeEach
    public void setUp() {
        s3Client = S3ClientFactory.createDefaultClient();
        S3ClientUtils.ensureBucketExists(s3Client, BUCKET_NAME);
        this.catalogService = new CatalogService(s3Client, BUCKET_NAME);
        this.tableName = "catalog_service_test_" + System.currentTimeMillis();
    }

    @AfterEach
    public void tearDown() {
        if ("true".equals(System.getProperty("keepData"))) {
            return;
        }
        S3ClientUtils.deleteTable(s3Client, BUCKET_NAME, tableName);
    }

    @Test
    public void testTableMetadataVersioningAndEvolutionRoundTrip() throws Exception {
        // 1. Verify table does not exist initially
        assertFalse(catalogService.isTableExists(tableName));

        // --- STEP 2: Upload Dummy Physical Data Files to S3 First ---
        // Simulating the writer creating physical data files before registering them in metadata
        String pathV1 = "tables/" + tableName + "/data/part-001.parquet";
        String pathV2 = "tables/" + tableName + "/data/part-002.parquet";

        s3Client.putObject(
            PutObjectRequest.builder().bucket(BUCKET_NAME).key(pathV1).build(),
            RequestBody.fromBytes("mock-parquet-data-v1".getBytes(StandardCharsets.UTF_8))
        );

        s3Client.putObject(
            PutObjectRequest.builder().bucket(BUCKET_NAME).key(pathV2).build(),
            RequestBody.fromBytes("mock-parquet-data-v2".getBytes(StandardCharsets.UTF_8))
        );

        // --- STEP 3: Build & Save v1.0 Metadata ---
        Field orderId = new Field(1, "order_id", "long", true);
        Field amount = new Field(2, "amount", "double", false);
        Schema v1Schema = new Schema(1, List.of(orderId, amount));

        DataFile fileV1 = new DataFile(pathV1, 1000, 30000L);
        Snapshot snapshotV1 = new Snapshot(101, System.currentTimeMillis(), null, List.of(fileV1));

        TableMetadata v1Metadata = new TableMetadata(
                1,
                UUID.randomUUID().toString(),
                "s3://" + BUCKET_NAME + "/tables/" + tableName,
                v1Schema,
                (long) 101,
                List.of(snapshotV1)
        );

        int savedVersion1 = catalogService.saveTableMetadata(tableName, v1Metadata);
        assertEquals(1, savedVersion1, "First save should commit as v1.json");
        assertTrue(catalogService.isTableExists(tableName));

        // --- STEP 4: Build & Save v2.0 Metadata (Schema Evolution & Lineage) ---
        Field email = new Field(3, "customer_email", "string", false);
        Schema v2Schema = new Schema(2, List.of(orderId, amount, email));

        DataFile fileV2 = new DataFile(pathV2, 500, 15000L);
        Snapshot snapshotV2 = new Snapshot(
                102,
                System.currentTimeMillis() + 1000,
                (long) 101,
                List.of(fileV2)
        );

        TableMetadata v2Metadata = new TableMetadata(
                1,
                v1Metadata.getTableUuid(),
                "s3://" + BUCKET_NAME + "/tables/" + tableName,
                v2Schema,
                (long) 102,
                List.of(snapshotV1, snapshotV2)
        );

        int savedVersion2 = catalogService.saveTableMetadata(tableName, v2Metadata);
        assertEquals(2, savedVersion2, "Second save should commit as v2.json");

        // --- STEP 5: Test "Load Latest" & Complete Metadata-to-Data Read Process ---
        TableMetadata latestMetadata = catalogService.loadTableMetadata(tableName);
        assertNotNull(latestMetadata);
        assertEquals(2, latestMetadata.getSchema().getSchemaId());
        assertEquals(3, latestMetadata.getSchema().getFields().size());
        assertEquals((long) 102, latestMetadata.getCurrentSnapshotId());

        // THE READER FLOW: Traverse metadata to find active files, then read them from S3
        for (Snapshot snapshot : latestMetadata.getSnapshots()) {
            for (DataFile dataFile : snapshot.getAddedFiles()) {
                // Read physical data file using path extracted from metadata manifest
                ResponseBytes<GetObjectResponse> dataContent = s3Client.getObjectAsBytes(
                    software.amazon.awssdk.services.s3.model.GetObjectRequest.builder()
                        .bucket(BUCKET_NAME)
                        .key(dataFile.getFilePath())
                        .build()
                );
                
                assertNotNull(dataContent);
                assertTrue(dataContent.asUtf8String().contains("mock-parquet-data"));
            }
        }

        // --- STEP 6: Test Specific Version Loading (Time-Travel / Auditing) ---
        TableMetadata v1Loaded = catalogService.loadTableMetadata(tableName, 1);
        assertNotNull(v1Loaded);
        assertEquals(1, v1Loaded.getSchema().getSchemaId());
        assertEquals(2, v1Loaded.getSchema().getFields().size());
        assertEquals(101, v1Loaded.getCurrentSnapshotId());
        assertNull(v1Loaded.getSnapshots().get(0).getParentSnapshotId());

        // Verify time-travel reads only v1's data file and ignores v2's file
        assertEquals(1, v1Loaded.getSnapshots().get(0).getAddedFiles().size());
        String v1FilePath = v1Loaded.getSnapshots().get(0).getAddedFiles().get(0).getFilePath();
        assertEquals(pathV1, v1FilePath);
    }
}