package xsgao.lakehouseconnector;

import java.time.Duration;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import xsgao.lakehouseconnector.utils.S3ClientFactory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Standalone CI capability test using Testcontainers.
 *
 * <p>The rest of the suite still targets the local MinIO started by {@code docker compose}.
 * This class starts its own disposable MinIO, discovers the mapped host port, and verifies a
 * basic S3 round trip. It proves the tests can run without an externally started container
 * while leaving the existing local-development workflow untouched.
 */
public class IntegrationTest {

    private static final String MINIO_IMAGE = "cgr.dev/chainguard/minio:latest";
    private static final int MINIO_PORT = 9000;
    private static final String ACCESS_KEY = "admin";
    private static final String SECRET_KEY = "password123";

    static {
        // Docker Engine 29+ rejects docker-java's default client API (1.32); 1.44 is
        // supported by current Docker Desktop and GitHub-hosted runners.
        System.setProperty("api.version", System.getProperty("api.version", "1.44"));
    }

    private static final GenericContainer<?> MINIO =
            new GenericContainer<>(DockerImageName.parse(MINIO_IMAGE))
                    .withEnv("MINIO_ROOT_USER", ACCESS_KEY)
                    .withEnv("MINIO_ROOT_PASSWORD", SECRET_KEY)
                    .withCommand("server", "/data")
                    .withExposedPorts(MINIO_PORT)
                    .waitingFor(Wait.forHttp("/minio/health/live")
                            .forPort(MINIO_PORT)
                            .withStartupTimeout(Duration.ofSeconds(90)));

    @BeforeAll
    static void startMinio() {
        assumeTrue(DockerClientFactory.instance().isDockerAvailable(),
                "Docker is not available; skipping the Testcontainers demo");
        MINIO.start();
        System.out.printf("[Testcontainers] MinIO started at %s%n", endpoint());
    }

    @AfterAll
    static void stopMinio() {
        MINIO.stop();
    }

    @Test
    void testEphemeralMinioS3RoundTrip() {
        String bucket = "ci-integration-" + System.nanoTime();
        String key = "hello.txt";
        String payload = "hello-lakehouse-ci";

        try (S3Client s3 = S3ClientFactory.createMinIoClient(endpoint())) {
            s3.createBucket(b -> b.bucket(bucket));
            s3.putObject(b -> b.bucket(bucket).key(key),
                    RequestBody.fromString(payload));

            String readBack = s3.getObjectAsBytes(b -> b.bucket(bucket).key(key)).asUtf8String();
            assertThat(readBack).isEqualTo(payload);

            s3.deleteObject(b -> b.bucket(bucket).key(key));
            s3.deleteBucket(b -> b.bucket(bucket));
        }
    }

    private static String endpoint() {
        return "http://" + MINIO.getHost() + ":" + MINIO.getMappedPort(MINIO_PORT);
    }
}
