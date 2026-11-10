package xsgao.lakehouseconnector.utils;

import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.core.exception.SdkException;

import java.io.IOException;
import java.util.Random;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

public class S3RetryExecutor {
    private static final Logger LOGGER = Logger.getLogger(S3RetryExecutor.class.getName());
    
    private final int maxRetries;
    private final long baseDelayMs;
    private final long maxDelayMs;
    private final Random random = new Random();

    public S3RetryExecutor() {
        this.maxRetries = Config.S3_MAX_RETRIES;
        this.baseDelayMs = Config.S3_BASE_DELAY_MS;
        this.maxDelayMs = Config.S3_MAX_DELAY_MS;
    }

    /**
     * Executes an S3 storage operation with exponential backoff and jitter.
     */
    public <T> T execute(Supplier<T> operation, String operationName) throws Exception {
        if (maxRetries <= 0) {
            // No retries configured, execute once
            return operation.get();
        }

        int attempt = 0;
        while (true) {
            try {
                return operation.get();
            } catch (Exception e) {
                attempt++;
                if (attempt > maxRetries || !isRetryable(e)) {
                    LOGGER.log(Level.SEVERE, "Operation [{0}] failed permanently after {1} attempts.", 
                        new Object[]{operationName, attempt});
                    throw e;
                }

                long sleepTimeMs = calculateBackoffWithJitter(attempt);
                LOGGER.log(Level.WARNING, "Operation [{0}] encountered transient error (attempt {1}/{2}). Retrying in {3}ms...",
                        new Object[]{operationName, attempt, maxRetries, sleepTimeMs});

                try {
                    Thread.sleep(sleepTimeMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Retry interrupted for operation: " + operationName, ie);
                }
            }
        }
    }

    private boolean isRetryable(Exception e) {
        if (e instanceof S3Exception s3Ex) {
            int statusCode = s3Ex.statusCode();
            // Retry on S3 SlowDown (503), Too Many Requests (429), and server errors (500, 502, 504)
            return statusCode == 429 || statusCode == 503 || statusCode == 500 || statusCode == 502 || statusCode == 504;
        }
        // Retry on transient network/SDK or IO exceptions
        return e instanceof SdkException || e instanceof IOException;
    }

    private long calculateBackoffWithJitter(int attempt) {
        // Exponential backoff: baseDelay * 2^(attempt - 1)
        long exponentialDelay = baseDelayMs * (1L << Math.min(attempt - 1, 30));
        long cappedDelay = Math.min(exponentialDelay, maxDelayMs);
        
        // Full jitter: uniformly distributed random value between 0 and cappedDelay
        return (long) (random.nextDouble() * cappedDelay);
    }
}