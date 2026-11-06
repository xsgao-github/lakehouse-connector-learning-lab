package xsgao.lakehouseconnector.utils;

import java.net.URI;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

public class S3ClientFactory {

    public static final String MINIO_ACCESS_KEY = "admin";
    public static final String MINIO_SECRET_KEY = "password123";

    public static S3Client createMinIoClient(String endpointOverride) {
        return S3Client.builder()
                .endpointOverride(URI.create(endpointOverride)) //Only required for testing with MinIO, as it does not use standard AWS endpoints
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create("admin", "password123")))
                .forcePathStyle(true) // Only required for testing with MinIO, as it does not support virtual-hosted style requests
                .build();

        // Production S3 client creation can be done using the default client:
        // return S3Client.builder()
        //         // 1. Region comes from deployment configuration
        //         .region(Region.of(regionName))
                
        //         // 2. Uses AWS IAM roles, environment variables, or ~/.aws/credentials
        //         .credentialsProvider(DefaultCredentialsProvider.create())
                
        //         // 3. NO endpoint override (SDK uses standard AWS endpoints)
        //         // 4. NO forcePathStyle (defaults to secure virtual-hosted style)
        //         .build();
    }

    public static S3Client createDefaultClient() {
        return createMinIoClient("http://localhost:9000");
    }
}