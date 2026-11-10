package xsgao.lakehouseconnector.writer;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import xsgao.lakehouseconnector.catalog.CatalogService;

public class OCCCommitHandler {

    private static final Logger LOGGER = Logger.getLogger(OCCCommitHandler.class.getName());

    private final S3Client s3Client;
    private final String bucketName;
    private final int maxRetries;
    private final long backoffMillis;

    public OCCCommitHandler(S3Client s3Client, String bucketName, int maxRetries, long backoffMillis) {
        this.s3Client = s3Client;
        this.bucketName = bucketName;
        this.maxRetries = maxRetries;
        this.backoffMillis = backoffMillis;
    }

    public int commitWithOCC(String tableName, Set<String> modifiedPartitions, VersionPayloadGenerator payloadSupplier) {
        int attempt = 0;

        while (attempt < maxRetries) {
            attempt++;
            try {
                CatalogService catalogService = new CatalogService(s3Client, bucketName);
                int currentVersion = catalogService.discoverLatestVersion(tableName);
                int targetVersion = currentVersion + 1;

                LOGGER.info(String.format("Attempt %d/%d: Trying to commit version v%d for table [%s]", 
                        attempt, maxRetries, targetVersion, tableName));

                String commitJson = payloadSupplier.generatePayload(targetVersion);
                boolean committed = executeAtomicPut(tableName, targetVersion, commitJson);

                if (committed) {
                    LOGGER.info(String.format("Successfully committed version v%d for table [%s]", targetVersion, tableName));
                    return targetVersion;
                }

                LOGGER.warning(String.format("Conflict detected on v%d for table [%s]. Another writer committed first. Retrying...", targetVersion, tableName));

            } catch (Exception e) {
                LOGGER.log(Level.WARNING, "Error encountered during OCC commit attempt: " + attempt, e);
                if (attempt >= maxRetries) {
                    throw new RuntimeException("OCC Commit failed: Exceeded maximum retry limit (" + maxRetries + ")", e);
                }
            }

            try {
                Thread.sleep(backoffMillis * attempt);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("OCC commit loop interrupted.", ie);
            }
        }
        throw new RuntimeException("OCC Commit failed: Exceeded maximum retry limit (" + maxRetries + ")");
    }

    private boolean executeAtomicPut(String tableName, int targetVersion, String jsonPayload) {
        String objectKey = String.format("tables/%s/metadata/v%d.json", tableName.toLowerCase(), targetVersion);

        PutObjectRequest putRequest = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(objectKey)
                .overrideConfiguration(c -> c.putHeader("If-None-Match", "*"))
                .contentType("application/json")
                .build();

        try {
            s3Client.putObject(putRequest, RequestBody.fromBytes(jsonPayload.getBytes(StandardCharsets.UTF_8)));
            return true;
        } catch (S3Exception e) {
            if (e.statusCode() == 412 || "PreconditionFailed".equals(e.awsErrorDetails().errorCode())) {
                return false; 
            }
            throw e;
        }
    }

    public void promoteStagingFiles(String tableName, String txnUuid, List<String> relativeFilePaths) {
        String stagingPrefix = String.format("tables/%s/_staging/txn_%s/", tableName.toLowerCase(), txnUuid);
        String dataPrefix = String.format("tables/%s/data/txn_%s/", tableName.toLowerCase(), txnUuid);

        for (String fileName : relativeFilePaths) {
            String sourceKey = stagingPrefix + fileName;
            String destinationKey = dataPrefix + fileName;

            CopyObjectRequest copyReq = CopyObjectRequest.builder()
                    .sourceBucket(bucketName)
                    .sourceKey(sourceKey)
                    .destinationBucket(bucketName)
                    .destinationKey(destinationKey)
                    .build();
            s3Client.copyObject(copyReq);

            DeleteObjectRequest deleteReq = DeleteObjectRequest.builder()
                    .bucket(bucketName)
                    .key(sourceKey)
                    .build();
            s3Client.deleteObject(deleteReq);
        }

        LOGGER.info(String.format("Successfully promoted files to data/txn_%s/ for table [%s]", txnUuid, tableName));
    }
    @FunctionalInterface
    public interface VersionPayloadGenerator {
        String generatePayload(int targetVersion);
    }
}