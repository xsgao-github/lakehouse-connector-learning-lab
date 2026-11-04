package xsgao.lakehouseconnector.catalog;

import java.nio.charset.StandardCharsets;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

/**
 * Service for managing table metadata with storage-centric design.
 * Uses MinIO/S3 for storing versioned JSON metadata files.
 */
public class CatalogService {

    private static final Logger LOGGER = Logger.getLogger(CatalogService.class.getName());

    // Jackson ObjectMapper singleton with Java time support
    private static final ObjectMapper OBJECT_MAPPER;
    static {
        OBJECT_MAPPER = new ObjectMapper();
        OBJECT_MAPPER.registerModule(new JavaTimeModule());
    }

    private final S3Client s3Client;
    private final String bucketName;
    
    public CatalogService(S3Client s3Client, String bucketName) {
        this.s3Client = s3Client;
        this.bucketName = bucketName;
    }

    /**
     * Checks if a table exists in the catalog by verifying the presence of any metadata version in S3.
     * @param tableName the name of the table to check
     * @return true if the table exists, false otherwise
     */
    public boolean isTableExists(String tableName) {
        String prefix = buildMetadataPrefix(tableName);
        try {
            ListObjectsV2Request request = ListObjectsV2Request.builder()
                    .bucket(bucketName)
                    .prefix(prefix + "/v")
                    .maxKeys(1)
                    .build();
            ListObjectsV2Response response = s3Client.listObjectsV2(request);
            return response.contents() != null && !response.contents().isEmpty();
        } catch (AwsServiceException | SdkClientException e) {
            return false;
        }
    }

    /**
     * Loads the latest table metadata from S3 by discovering the highest vN.json version.
     * @param tableName the name of the table whose metadata is to be loaded
     * @return the TableMetadata object representing the table's latest metadata
     * @throws Exception if an error occurs while loading the metadata
     */
    public TableMetadata loadTableMetadata(String tableName) throws Exception {
        int latestVersion = discoverLatestVersion(tableName);
        if (latestVersion == 0) {
            throw new IllegalStateException("No metadata versions found for table: " + tableName);
        }
        return loadTableMetadata(tableName, latestVersion);
    }

    /**
     * Loads a specific version of the table metadata from S3.
     * @param tableName the name of the table
     * @param version the specific metadata version number (e.g., 1 for v1.json)
     * @return the TableMetadata object
     * @throws Exception if an error occurs while loading
     */
    public TableMetadata loadTableMetadata(String tableName, int version) throws Exception {
        String key = buildVersionedMetadataKey(tableName, version);
        ResponseBytes<software.amazon.awssdk.services.s3.model.GetObjectResponse> response = s3Client.getObject(
                GetObjectRequest.builder()
                        .bucket(bucketName)
                        .key(key)
                        .build(),
                software.amazon.awssdk.core.sync.ResponseTransformer.toBytes());

        byte[] jsonBytes = response.asByteArray();
        return TableMetadata.fromJson(new String(jsonBytes, StandardCharsets.UTF_8));
    }

    /**
     * Automatically determines the next version number and saves the metadata as vN.json.
     * @param tableName the name of the table whose metadata is to be saved
     * @param metadata the TableMetadata object representing the table's metadata
     * @return the version number (N) of the saved metadata
     * @throws Exception if an error occurs while saving the metadata
     */
    public int saveTableMetadata(String tableName, TableMetadata metadata) throws Exception {
        int nextVersion = discoverLatestVersion(tableName) + 1;
        String key = buildVersionedMetadataKey(tableName, nextVersion);
        String jsonContent = metadata.toJson();
        byte[] jsonBytes = jsonContent.getBytes(StandardCharsets.UTF_8);

        s3Client.putObject(
                PutObjectRequest.builder()
                        .bucket(bucketName)
                        .key(key)
                        .contentType("application/json")
                        .build(),
                software.amazon.awssdk.core.sync.RequestBody.fromBytes(jsonBytes));
        return nextVersion;
    }

    /**
     * Stages metadata in a temporary transaction folder.
     * Part of the WAP (Write-Ahead-Parquet) pattern.
     *
     * @param tableName the name of the table
     * @param txnId the transaction ID (e.g., uuid)
     * @param metadata the metadata to stage
     * @param version the intended version number
     * @throws Exception if an error occurs while staging
     */
    public void stageTableMetadata(String tableName, String txnId, TableMetadata metadata, int version) throws Exception {
        String stagingKey = String.format("tables/%s/_staging/txn_%s/v%d.json",
            tableName.toLowerCase(), txnId, version);

        String jsonContent = metadata.toJson();
        byte[] jsonBytes = jsonContent.getBytes(StandardCharsets.UTF_8);

        s3Client.putObject(
                PutObjectRequest.builder()
                        .bucket(bucketName)
                        .key(stagingKey)
                        .contentType("application/json")
                        .build(),
                software.amazon.awssdk.core.sync.RequestBody.fromBytes(jsonBytes));
    }

    /**
     * Cleans up a transaction staging folder.
     */
    public void cleanupStaging(String tableName, String txnId) {
        String prefix = String.format("tables/%s/_staging/txn_%s/",
            tableName.toLowerCase(), txnId);
        try {
            ListObjectsV2Response listRes = s3Client.listObjectsV2(
                ListObjectsV2Request.builder().bucket(bucketName).prefix(prefix).build());

            if (listRes.contents() != null && !listRes.contents().isEmpty()) {
                java.util.List<software.amazon.awssdk.services.s3.model.ObjectIdentifier> ids =
                    listRes.contents().stream()
                        .map(obj -> software.amazon.awssdk.services.s3.model.ObjectIdentifier.builder().key(obj.key()).build())
                        .collect(java.util.stream.Collectors.toList());

                s3Client.deleteObjects(software.amazon.awssdk.services.s3.model.DeleteObjectsRequest.builder()
                        .bucket(bucketName)
                        .delete(software.amazon.awssdk.services.s3.model.Delete.builder().objects(ids).build())
                        .build());
            }
        } catch (Exception ignored) {}
    }

    /**
     * Discovers the highest existing version number (vN.json) for a table in S3.
     */
    public int discoverLatestVersion(String tableName) {
        String metadataPrefix = buildMetadataPrefix(tableName) + "/";
        try {
            software.amazon.awssdk.services.s3.model.ListObjectsV2Response response = s3Client.listObjectsV2(
                    software.amazon.awssdk.services.s3.model.ListObjectsV2Request.builder()
                            .bucket(bucketName)
                            .prefix(metadataPrefix)
                            .build()
            );

            int maxVersion = 0; // Default to 0 if no metadata exists yet
            for (software.amazon.awssdk.services.s3.model.S3Object obj : response.contents()) {
                String key = obj.key();
                // Matches keys like warehouse/tables/orders/metadata/v1.json
                if (key.matches(".*/v\\d+\\.json$")) {
                    String fileName = key.substring(key.lastIndexOf('/') + 1);
                    String verStr = fileName.replaceAll("[^0-9]", "");
                    try {
                        int ver = Integer.parseInt(verStr);
                        if (ver > maxVersion) {
                            maxVersion = ver;
                        }
                    } catch (NumberFormatException ignored) {}
                }
            }
            return maxVersion;
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Failed to discover latest version, defaulting to 0", e);
            return 0;
        }
    }

    private static String buildMetadataPrefix(String tableName) {
        return String.format("tables/%s/metadata", tableName.toLowerCase());
    }

    private static String buildVersionedMetadataKey(String tableName, int version) {
        return String.format("%s/v%d.json", buildMetadataPrefix(tableName), version);
    }
}