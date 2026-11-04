package xsgao.lakehouseconnector.writer;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import software.amazon.awssdk.services.s3.S3Client;
import xsgao.lakehouseconnector.catalog.CatalogService;
import xsgao.lakehouseconnector.catalog.DataFile;
import xsgao.lakehouseconnector.catalog.Schema;
import xsgao.lakehouseconnector.catalog.Snapshot;
import xsgao.lakehouseconnector.catalog.TableMetadata;

public class TableWriterService {
    private final S3Client s3Client;
    private final String bucketName;
    private final OCCCommitHandler commitHandler;

    public TableWriterService(S3Client s3Client, String bucketName, int maxRetries, long backoffMillis) {
        this.s3Client = s3Client;
        this.bucketName = bucketName;
        this.commitHandler = new OCCCommitHandler(s3Client, bucketName, maxRetries, backoffMillis);
    }

    /**
     * Commits a new table version using OCC with dynamic payload generation and partition tracking.
     */
    public int commitTableTransaction(
            String tableName, 
            Schema schema, 
            List<DataFile> newFiles, 
            Long parentSnapshotId) throws Exception {
        
        CatalogService catalogService = new CatalogService(s3Client, bucketName);

        // 1. Extract modified partitions from the new files (e.g., folder paths or partition keys)
        Set<String> modifiedPartitions = new HashSet<>();
        for (DataFile file : newFiles) {
            // Extract partition prefix if present in file path, or default to general table path
            modifiedPartitions.add(file.getFilePath());
        }

        // 2. Load current table metadata if table already exists
        final TableMetadata existingMetadata = catalogService.isTableExists(tableName) 
                ? catalogService.loadTableMetadata(tableName) 
                : null;

        // 3. Define the VersionPayloadGenerator lambda.
        // This is called dynamically inside OCCCommitHandler during each retry attempt,
        // ensuring the correct targetVersion is embedded into the metadata json.
        OCCCommitHandler.VersionPayloadGenerator payloadSupplier = (targetVersion) -> {
            long newSnapshotId = System.currentTimeMillis();
            Snapshot newSnapshot = new Snapshot(newSnapshotId, System.currentTimeMillis(), parentSnapshotId, newFiles);

            List<Snapshot> updatedSnapshots;
            if (existingMetadata != null) {
                updatedSnapshots = new java.util.ArrayList<>(existingMetadata.getSnapshots());
                updatedSnapshots.add(newSnapshot);
            } else {
                updatedSnapshots = List.of(newSnapshot);
            }

            TableMetadata newMetadata = new TableMetadata(
                1,
                existingMetadata != null ? existingMetadata.getTableUuid() : UUID.randomUUID().toString(),
                "s3://" + bucketName + "/tables/" + tableName,
                schema,
                newSnapshotId,
                updatedSnapshots
            );

            return newMetadata.toJson();
        };

        // 4. Execute the robust OCC commit handler with partition collision detection
        int committedVersion = commitHandler.commitWithOCC(tableName, modifiedPartitions, payloadSupplier);
        
        if (committedVersion == -1) {
            throw new RuntimeException("OCC commit failed after max retries for table: " + tableName);
        }

        return committedVersion;
    }
}