package xsgao.lakehouseconnector.utils;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import java.util.logging.Logger;
import java.util.logging.Level;

public class S3ClientUtils {
    private static final Logger logger = Logger.getLogger(S3ClientUtils.class.getName());

    public final static String BUCKET_NAME = "warehouse";

    public static void ensureBucketExists(S3Client s3Client, String bucketName) {
        try {
            s3Client.headBucket(HeadBucketRequest.builder().bucket(bucketName).build());
        } catch (NoSuchBucketException e) {
            s3Client.createBucket(CreateBucketRequest.builder().bucket(bucketName).build());
            logger.info("Created missing S3 bucket: " + bucketName);
        } catch (AwsServiceException | SdkClientException e) {
            // Handle other exceptions or custom MinIO initialization errors
            try {
                s3Client.createBucket(CreateBucketRequest.builder().bucket(bucketName).build());
            } catch (AwsServiceException | SdkClientException ex) {
                // Ignore if already exists or handle accordingly
            }
        }
    }

    public static void deleteTable(S3Client s3Client, String bucketName, String tableName) {
        if (tableName == null) {
            return;
        }

        // Delete all objects under the tableName prefix
        String prefix = "tables/" + tableName + "/";
        try {
            // 1. List all objects under this table's prefix
            software.amazon.awssdk.services.s3.model.ListObjectsV2Request listReq =
                software.amazon.awssdk.services.s3.model.ListObjectsV2Request.builder()
                    .bucket(bucketName)
                    .prefix(prefix)
                    .build();

            software.amazon.awssdk.services.s3.model.ListObjectsV2Response listRes = s3Client.listObjectsV2(listReq);

            if (listRes.contents() != null && !listRes.contents().isEmpty()) {
                // 2. Build delete requests for every object found
                java.util.List<software.amazon.awssdk.services.s3.model.ObjectIdentifier> objectsToDelete =
                    listRes.contents().stream()
                        .map(obj -> software.amazon.awssdk.services.s3.model.ObjectIdentifier.builder().key(obj.key()).build())
                        .collect(java.util.stream.Collectors.toList());

                software.amazon.awssdk.services.s3.model.DeleteObjectsRequest deleteReq =
                    software.amazon.awssdk.services.s3.model.DeleteObjectsRequest.builder()
                        .bucket(bucketName)
                        .delete(software.amazon.awssdk.services.s3.model.Delete.builder().objects(objectsToDelete).build())
                        .build();

                s3Client.deleteObjects(deleteReq);
            }
        } catch (AwsServiceException | SdkClientException e) {
            // Log or ignore cleanup errors to prevent masking actual test failures
            logger.log(Level.WARNING, "Warning: Failed to clean up S3 prefix for " + tableName + ": " + e.getMessage());
        }
    }
}