package xsgao.lakehouseconnector;

import java.nio.charset.StandardCharsets;

import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.BucketAlreadyExistsException;
import software.amazon.awssdk.services.s3.model.BucketAlreadyOwnedByYouException;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import xsgao.lakehouseconnector.utils.S3ClientFactory;

public class StorageSmokeTestApp {
    private final S3Client s3Client;
    private final String bucketName = "warehouse";

    public StorageSmokeTestApp() {
        this.s3Client = S3ClientFactory.createDefaultClient();
    }

    public void writeTestFile() {
        try {
            s3Client.createBucket(CreateBucketRequest.builder().bucket(bucketName).build());
        } catch (BucketAlreadyOwnedByYouException | BucketAlreadyExistsException e) {
            // Ignored
        }

        String csvContent = "order_id,amount,order_date\n1001,150.50,2026-11-01\n1002,99.99,2026-11-01\n";
        s3Client.putObject(
                PutObjectRequest.builder().bucket(bucketName).key("tables/orders.csv").build(),
                software.amazon.awssdk.core.sync.RequestBody.fromBytes(csvContent.getBytes(StandardCharsets.UTF_8))
        );
    }

    public String readTestFile() {
        ResponseBytes<GetObjectResponse> objectBytes = s3Client.getObject(
                GetObjectRequest.builder().bucket(bucketName).key("tables/orders.csv").build(),
                software.amazon.awssdk.core.sync.ResponseTransformer.toBytes()
        );
        return new String(objectBytes.asByteArray(), StandardCharsets.UTF_8);
    }
}