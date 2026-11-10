# Module 8: Robust Error Handling & S3 Resilience

## Overview

In cloud-native data lakehouses built on top of object stores like AWS S3 or MinIO, transient network glitches, socket timeouts, and throttling (`503 Slow Down`) are expected runtime conditions. 

Module 8 establishes production-grade error resilience across the Lakehouse Connector by implementing a centralized `S3RetryExecutor` that automatically retries transient cloud storage failures with exponential backoff and jitter, while failing fast on non-retryable client and security errors.

## Key Components

### 1. `S3RetryExecutor`

The `S3RetryExecutor` wraps S3 API requests in a configurable retry loop:

* **Transient (Retryable) Errors:**
  * HTTP `503` (Slow Down / Throttling)
  * HTTP `500` (Internal Server Error)
  * HTTP `502` (Bad Gateway)
  * HTTP `504` (Gateway Timeout)
  * Underlying network connection timeouts / resets (`IOException`)

* **Terminal (Non-Retryable) Errors:**
  * HTTP `404` (NoSuchKey / Resource Not Found)
  * HTTP `403` (AccessDenied / Invalid Credentials)
  * HTTP `400` (BadRequest / Invalid Query)
  * Fail fast immediately without wasting execution time or retry attempts.

### 2. Exponential Backoff with Full Jitter

To avoid thundering herd scenarios across distributed query executors, sleep intervals use a full jitter backoff formula:

$$
\text{Sleep Time} = \text{Random}\left(0, \min\left(\text{maxBackoffMs}, \text{baseBackoffMs} \times 2^{\text{attempt}}\right)\right)
$$

## Implementation Details

### Integrating Retry in `CatalogService`

`CatalogService` wraps AWS SDK v2 calls using `S3RetryExecutor` to guarantee metadata fetch resilience:

```java
public TableMetadata loadTableMetadata(String tableName, int version) throws Exception {
    String key = buildVersionedMetadataKey(tableName, version);
    
    ResponseBytes<GetObjectResponse> response = s3RetryExecutor.execute(() -> 
        s3Client.getObject(
            GetObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .build(),
            ResponseTransformer.toBytes()
        ), 
        "S3-GetObject-" + key
    );
    
    byte[] jsonBytes = response.asByteArray();
    return TableMetadata.fromJson(new String(jsonBytes, StandardCharsets.UTF_8));
}
```

## Verification Strategy

### Transient Recovery Test (`CatalogServiceTest`)

Transient fault tolerance is verified using Mockito by mocking `S3Client` to throw two consecutive `503 Slow Down` errors before succeeding on the third attempt:

```java
@Test
void testCatalogServiceRetrySuccessOnThirdAttempt() throws Exception {
    S3Client mockS3 = mock(S3Client.class);
    CatalogService mockCatalogService = new CatalogService(mockS3, BUCKET_NAME, true);

    S3Exception transientError = (S3Exception) S3Exception.builder()
            .statusCode(503)
            .message("Slow Down")
            .build();

    ResponseBytes<GetObjectResponse> mockResponse = ResponseBytes.fromByteArray(
        GetObjectResponse.builder().build(), 
        "{\"schemaId\": 1, \"fields\": [], \"tableUuid\": \"uuid\", \"location\": \"s3://bucket/table\", \"currentSnapshotId\": 101, \"snapshots\": []}".getBytes()
    );

    when(mockS3.getObject(any(GetObjectRequest.class), any(ResponseTransformer.class)))
            .thenThrow(transientError)
            .thenThrow(transientError)
            .thenReturn(mockResponse);

    TableMetadata metadata = mockCatalogService.loadTableMetadata("users", 1);

    assertNotNull(metadata);
    verify(mockS3, times(3)).getObject(any(GetObjectRequest.class), any(ResponseTransformer.class));
}
```
