# Module 11: Testcontainers Integration Test & GitHub Actions

## Topic

Adding one standalone Testcontainers test that provisions its own ephemeral MinIO, and running that test on GitHub Actions. The existing MinIO-backed suite stays local and continues to use `docker compose`.

## Two Execution Modes

| Mode | Storage | Used by |
|---|---|---|
| Local development | `docker compose up -d` on `localhost:9000` | Existing integration suite |
| CI | `IntegrationTest` starts a `cgr.dev/chainguard/minio` Testcontainer | GitHub Actions |

This keeps the existing suite and `S3ClientFactory` defaults untouched. The Testcontainers test passes its mapped endpoint directly to `S3ClientFactory.createMinIoClient(...)`.

## Key Concepts

* **Ephemeral container per test class**: `IntegrationTest` starts `cgr.dev/chainguard/minio` in `@BeforeAll`, waits for `/minio/health/live`, and stops it in `@AfterAll`.
* **Dynamic host port mapping**: the container exposes `9000`, but Docker assigns a random host port. The test reads `MINIO.getMappedPort(9000)` and passes the resulting URL to the S3 client.
* **S3 round trip**: the test creates a unique bucket, writes an object, reads it back, asserts the payload, and deletes the object and bucket.
* **No production refactor**: the existing suite keeps using `localhost:9000`; only the standalone test uses the container endpoint.
* **GitHub Actions**: `ubuntu-latest` provides the Docker socket. The workflow runs `mvn clean test -Dtest=IntegrationTest`; no external MinIO step is needed.
* **Maven cache + Arrow flag**: `actions/setup-java` caches `~/.m2`; `MAVEN_OPTS` opens `java.base/java.nio` for Arrow's off-heap allocator.

## Test Shape

```java
private static final GenericContainer<?> MINIO =
    new GenericContainer<>(DockerImageName.parse("cgr.dev/chainguard/minio:latest"))
        .withEnv("MINIO_ROOT_USER", "admin")
        .withEnv("MINIO_ROOT_PASSWORD", "password123")
        .withCommand("server", "/data")
        .withExposedPorts(9000)
        .waitingFor(Wait.forHttp("/minio/health/live").forPort(9000));

@BeforeAll static void startMinio() { MINIO.start(); }
@AfterAll  static void stopMinio()  { MINIO.stop(); }

String endpoint = "http://" + MINIO.getHost() + ":" + MINIO.getMappedPort(9000);
```

## CI Workflow

```text
checkout ──► setup JDK 17 + Maven cache ──► mvn clean test -Dtest=IntegrationTest
                                              └─ Testcontainers starts MinIO
                                                 on the runner's Docker daemon
```

The workflow contains no MinIO `docker run`, health-wait loop, or service container.

## Common Pitfalls & Solutions

| Pitfall | Solution |
|---|---|
| Running the full suite in CI without a local MinIO | CI runs only `IntegrationTest`; the existing suite stays local and uses `docker compose` |
| Hardcoding `9000` as the host port for the container | Always use `MINIO.getMappedPort(9000)`; the host port is random |
| Container starts but tests connect too early | Use `Wait.forHttp("/minio/health/live")` |
| Docker Engine API mismatch (`client version ... is too old`) | `IntegrationTest` sets `api.version=1.44` before startup because Docker 29+ rejects docker-java's default 1.32; override with `-Dapi.version=...` if your engine differs |
| Local Windows run cannot reach Docker | Start Docker Desktop; `IntegrationTest` needs only the Docker daemon, not `docker compose`. If Docker is unavailable, the test is skipped via a JUnit assumption |
| Arrow fails with `InaccessibleObjectException` on JDK 17 | Keep `--add-opens` in Surefire `argLine` and `MAVEN_OPTS` |

## Verification

Testcontainers test locally (Docker running, no compose required):

```bash
mvn test -Dtest=IntegrationTest
```

Full local suite (existing tests still need the fixture MinIO):

```bash
docker compose up -d
mvn clean test
```

CI:

1. push the change or open a pull request;
2. open the **Actions** tab and select **Lakehouse Connector CI**;
3. expect the JDK setup, the Testcontainers MinIO startup, and the S3 round trip to pass.

Expected log:

```text
[Testcontainers] MinIO started at http://localhost:32768
```
