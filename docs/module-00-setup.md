# Module 00: Environment & Storage Bootstrap

Welcome to **Module 00** of the Lakehouse Connector Learning Lab! In this module, we establish our foundational local storage layer, configure the AWS SDK v2 client for path-style access, and verify our setup using a self-contained storage smoke test.

---

## 1. Getting Started (Module 0)

1.1. **Start Local MinIO:**
   ```bash
   docker compose up -d
   ```

1.2. **Build and Test:**
   ```bash
   mvn clean test
   ```

---

## 2. Core S3 API Highlights (Java)

Instead of relying on external scripts for Module 00, our application self-provisions its environment and exercises the S3 API directly through Java.

### 2.1. Connecting via AWS SDK v2
To talk to local MinIO, we use `S3ClientFactory` to enforce **path-style access** and override the endpoint URL with static credentials.

```java
public class S3ClientFactory {
    public static S3Client createDefaultClient() {
        return S3Client.builder()
                .endpointOverride(URI.create("http://localhost:9000"))
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create("admin", "password123")
                ))
                .forcePathStyle(true) // Crucial for local S3-compatible stores
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
}
```

### 2.2. Writing and Reading Objects Programmatically
Our `StorageSmokeTestApp` demonstrates safe bucket creation (ignoring already-owned exceptions) and standard stream/byte transformations using AWS SDK v2's `RequestBody` and `ResponseTransformer`.

```java
public void writeTestFile() {
    try {
        s3Client.createBucket(CreateBucketRequest.builder().bucket(bucketName).build());
    } catch (BucketAlreadyOwnedByYouException | BucketAlreadyExistsException e) {
        // Ignored
    }

    String csvContent = "order_id,amount,order_date\n1001,150.50,2026-11-01\n1002,99.99,2026-11-01\n";
    s3Client.putObject(
            PutObjectRequest.builder().bucket(bucketName).key("smoke-test/orders.csv").build(),
            RequestBody.fromBytes(csvContent.getBytes(StandardCharsets.UTF_8))
    );
}

public String readTestFile() {
    ResponseBytes<GetObjectResponse> objectBytes = s3Client.getObject(
            GetObjectRequest.builder().bucket(bucketName).key("smoke-test/orders.csv").build(),
            ResponseTransformer.toBytes()
    );
    return new String(objectBytes.asByteArray(), StandardCharsets.UTF_8);
}
```

---

## 3. Common Pitfalls & Solutions

When working with local object storage and the AWS SDK v2, watch out for these frequent issues:

### 3.1. Missing Path-Style Access (`forcePathStyle(true)`)
* **Symptom:** Connection timeouts, `UnknownHostException`, or `400 Bad Request` when trying to execute S3 commands.
* **Cause:** By default, modern AWS SDKs attempt **virtual-hosted style** requests (e.g., `http://warehouse.localhost:9000/...`), looking for subdomains that local DNS doesn't resolve.
* **Solution:** Always set `.forcePathStyle(true)` when initializing clients targeting MinIO or LocalStack.

### 3.2. Port Confusion (API vs. Console)
* **Symptom:** XML parsing exceptions or strange HTML responses from your Java app.
* **Cause:** Pointing your SDK endpoint to MinIO's web UI port (`9001`) instead of the S3 API port (`9000`).
* **Solution:** Ensure your endpoint override is explicitly set to `http://localhost:9000`.

### 3.3. Lingering Corporate/Cloud Credentials
* **Symptom:** `403 Forbidden` (Access Denied) errors even though your MinIO server is running.
* **Cause:** The AWS SDK falls back to environment variables or local credentials files (`~/.aws/credentials`) if they exist on your machine, overriding your explicit MinIO builder credentials.
* **Solution:** Always use a `StaticCredentialsProvider` with explicit local credentials (`admin` / `password123`) for local development environments.

### 3.4. Unhandled Bucket Existence Exceptions
* **Symptom:** Application crashes with `BucketAlreadyExistsException` or `BucketAlreadyOwnedByYouException` on the second run.
* **Cause:** Attempting to provision a bucket programmatically without handling the case where it was already created during a previous execution.
* **Solution:** Wrap your bucket creation calls in a try-catch block to gracefully ignore these specific exceptions:
  ```java
  try {
      s3Client.createBucket(CreateBucketRequest.builder().bucket("warehouse").build());
  } catch (BucketAlreadyOwnedByYouException | BucketAlreadyExistsException e) {
      // Safe to ignore on subsequent runs
  }
  ```

### 3.5. Leading Slashes in Object Keys
* **Symptom**: Files showing up under an unintended empty folder root or failing prefix scans.
* **Cause**: Unlike traditional file systems, S3 keys are flat strings. Specifying /smoke-test/orders.csv (with a leading slash) creates an object whose name literally starts with /, rather than placing it inside a smoke-test prefix.
* **Solution**: Always omit leading slashes when defining object keys (e.g., use smoke-test/orders.csv).