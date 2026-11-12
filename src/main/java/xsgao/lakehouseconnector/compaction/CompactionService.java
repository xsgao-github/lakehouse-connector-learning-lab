package xsgao.lakehouseconnector.compaction;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.fs.Path;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;

import xsgao.lakehouseconnector.catalog.CatalogService;
import xsgao.lakehouseconnector.catalog.ColumnStats;
import xsgao.lakehouseconnector.catalog.DataFile;
import xsgao.lakehouseconnector.catalog.Schema;
import xsgao.lakehouseconnector.catalog.Snapshot;
import xsgao.lakehouseconnector.catalog.TableMetadata;
import xsgao.lakehouseconnector.compaction.CompactionPlanner.Group;
import xsgao.lakehouseconnector.parquet.ParquetTableReader;
import xsgao.lakehouseconnector.parquet.ParquetTableWriter;
import xsgao.lakehouseconnector.utils.S3RetryExecutor;
import xsgao.lakehouseconnector.writer.TableWriterService;

/**
 * Rewrites small data files into fewer larger files and publishes a new snapshot that
 * references the complete post-compaction live file set.
 *
 * <p>The catalog model stores only {@code added-files} per snapshot and the planner reads only
 * the active snapshot, so the commit must carry untouched plus compacted files. Old files stay
 * in S3 for time travel; this service does not vacuum them.
 */
public class CompactionService {

    private static final Logger LOGGER = Logger.getLogger(CompactionService.class.getName());

    private static final int DEFAULT_MAX_RETRIES = 3;
    private static final long DEFAULT_BACKOFF_MILLIS = 50;

    private final S3Client s3Client;
    private final String bucketName;
    private final CatalogService catalogService;
    private final TableWriterService tableWriterService;
    private final S3RetryExecutor s3RetryExecutor = new S3RetryExecutor();
    private final CompactionPlanner planner = new CompactionPlanner();

    public CompactionService(S3Client s3Client, String bucketName) {
        this(s3Client, bucketName, DEFAULT_MAX_RETRIES, DEFAULT_BACKOFF_MILLIS);
    }

    public CompactionService(S3Client s3Client, String bucketName, int maxRetries, long backoffMillis) {
        this.s3Client = s3Client;
        this.bucketName = bucketName;
        this.catalogService = new CatalogService(s3Client, bucketName);
        this.tableWriterService = new TableWriterService(s3Client, bucketName, maxRetries, backoffMillis);
    }

    /**
     * Runs one compaction pass over the table's active snapshot.
     *
     * @return metrics for the pass; {@code committed == false} when no file qualified
     */
    public Result compactTable(String tableName, CompactionOptions options) throws Exception {
        TableMetadata metadata = catalogService.loadTableMetadata(tableName);
        Snapshot activeSnapshot = findActiveSnapshot(metadata);
        List<DataFile> activeFiles = metadata.resolveLiveFiles();

        int filesBefore = activeFiles.size();
        long activeSnapshotBytesBefore = activeSnapshot == null
                ? 0
                : activeSnapshot.toJson().getBytes(StandardCharsets.UTF_8).length;
        long metadataJsonBytesBefore = metadata.toJson().getBytes(StandardCharsets.UTF_8).length;

        List<Group> groups = planner.plan(activeFiles, options);
        if (groups.isEmpty()) {
            LOGGER.info(String.format(
                    "**** [COMPACTION NOOP] table [%s] no qualifying groups (files=%d, threshold=%,d bytes, minInputFiles=%d) *****",
                    tableName, filesBefore, options.getSmallFileThresholdBytes(),
                    options.getMinInputFilesPerGroup()));
            return new Result(false, 0, metadata.getCurrentSnapshotId(), filesBefore, filesBefore,
                    activeSnapshotBytesBefore, activeSnapshotBytesBefore,
                    metadataJsonBytesBefore, metadataJsonBytesBefore,
                    0, 0, 0, 0, 0, 0, List.of());
        }

        int plannedBins = groups.stream().mapToInt(group -> group.bins().size()).sum();
        LOGGER.info(String.format(
                "**** [COMPACTION PLAN] table [%s] files=%d qualifyingGroups=%d bins=%d smallFiles=%d target=%,d bytes *****",
                tableName, filesBefore, groups.size(), plannedBins,
                groups.stream().mapToInt(Group::inputFileCount).sum(), options.getTargetFileSizeBytes()));

        String runId = UUID.randomUUID().toString().replace("-", "").substring(0, 8);
        List<DataFile> compactedFiles = new ArrayList<>();
        List<String> createdKeys = new ArrayList<>();
        Set<String> consumedPaths = new HashSet<>();
        int outputIndex = 0;
        int inputFilesConsumed = 0;
        long inputBytes = 0;
        long recordsRewritten = 0;
        boolean commitStarted = false;

        try {
            for (Group group : groups) {
                for (List<DataFile> bin : group.bins()) {
                    String relativeOutputPath = outputPath(group.partition(), runId, outputIndex++);
                    createdKeys.add(tableKey(tableName, relativeOutputPath));

                    DataFile output = rewriteBin(tableName, metadata, bin, relativeOutputPath, options);
                    compactedFiles.add(output);
                    recordsRewritten += output.getRecordCount();

                    for (DataFile input : bin) {
                        consumedPaths.add(input.getFilePath());
                        inputFilesConsumed++;
                        inputBytes += input.getFileSizeInBytes();
                    }
                }
            }

            // Publish a real delta: compacted outputs are added, consumed files are removed.
            // The planner replays snapshots to reconstruct the live set.
            List<DataFile> removedFiles = new ArrayList<>();
            for (DataFile file : activeFiles) {
                if (consumedPaths.contains(file.getFilePath())) {
                    removedFiles.add(file);
                }
            }
            int filesAfter = activeFiles.size() - removedFiles.size() + compactedFiles.size();

            commitStarted = true;
            int newVersion = tableWriterService.commitTableTransaction(
                    tableName, metadata.getSchema(), compactedFiles, removedFiles,
                    metadata.getCurrentSnapshotId());

            TableMetadata committedMetadata = catalogService.loadTableMetadata(tableName, newVersion);
            Snapshot committedSnapshot = findActiveSnapshot(committedMetadata);
            long activeSnapshotBytesAfter = committedSnapshot == null
                    ? 0
                    : committedSnapshot.toJson().getBytes(StandardCharsets.UTF_8).length;
            long metadataJsonBytesAfter = committedMetadata.toJson().getBytes(StandardCharsets.UTF_8).length;

            long outputBytes = 0;
            for (DataFile file : compactedFiles) {
                outputBytes += file.getFileSizeInBytes();
            }

            LOGGER.info(String.format(
                    "**** [COMPACTION COMMIT] table [%s] version=%d parentSnapshotId=%s files %d -> %d (untouched=%d compacted=%d) *****",
                    tableName, newVersion, metadata.getCurrentSnapshotId(), filesBefore,
                    filesAfter, filesAfter - compactedFiles.size(), compactedFiles.size()));
            LOGGER.info(String.format(
                    "**** [COMPACTION METRICS] entries %d -> %d, active snapshot JSON %,d -> %,d bytes, metadata JSON %,d -> %,d bytes *****",
                    filesBefore, filesAfter, activeSnapshotBytesBefore, activeSnapshotBytesAfter,
                    metadataJsonBytesBefore, metadataJsonBytesAfter));

            return new Result(true, newVersion, metadata.getCurrentSnapshotId(), filesBefore,
                    filesAfter, activeSnapshotBytesBefore, activeSnapshotBytesAfter,
                    metadataJsonBytesBefore, metadataJsonBytesAfter, groups.size(), inputFilesConsumed,
                    compactedFiles.size(), inputBytes, outputBytes, recordsRewritten,
                    List.copyOf(compactedFiles));

        } catch (Exception e) {
            if (commitStarted) {
                // The atomic metadata PUT may have succeeded before the client saw the error;
                // deleting outputs could corrupt a committed snapshot, so leave them in place.
                LOGGER.log(Level.WARNING, String.format(
                        "**** [COMPACTION WARN] table [%s] failure after commit started; %d output file(s) may be orphaned *****",
                        tableName, createdKeys.size()), e);
            } else {
                cleanupCreatedOutputs(tableName, createdKeys);
            }
            throw e;
        }
    }

    /**
     * Streams every record from a bin into one new Parquet file, then recovers the output's
     * footer statistics and real byte size for the catalog entry.
     */
    private DataFile rewriteBin(String tableName, TableMetadata metadata, List<DataFile> bin,
                                String relativeOutputPath, CompactionOptions options) throws Exception {
        Schema logicalSchema = metadata.getSchema();
        Path outputPath = s3aPath(tableName, relativeOutputPath);
        long records = 0;

        try (ParquetTableWriter writer = new ParquetTableWriter(
                logicalSchema, outputPath, options.getRowGroupSizeBytes(), options.getCodec())) {
            for (DataFile input : bin) {
                Path inputPath = s3aPath(tableName, input.getFilePath());
                Schema fileSchema = metadata.findSchema(input.getSchemaId());
                try (ParquetTableReader reader = new ParquetTableReader(fileSchema, logicalSchema, null, inputPath)) {
                    Map<String, Object> record;
                    while ((record = reader.readNextRecord()) != null) {
                        writer.writeRecord(record);
                        records++;
                    }
                }
            }
        }

        Map<String, ColumnStats> columnStats;
        try (ParquetTableReader statsReader = new ParquetTableReader(logicalSchema, null, outputPath)) {
            // Footer statistics keep the compacted file prunable by SplitPlanner (Module 3).
            columnStats = statsReader.extractFooterStatistics(outputPath);
        }

        String outputKey = tableKey(tableName, relativeOutputPath);
        long size = s3RetryExecutor.execute(
                () -> s3Client.headObject(b -> b.bucket(bucketName).key(outputKey)).contentLength(),
                "S3-HeadObject-" + outputKey);

        LOGGER.info(String.format(
                "**** [COMPACTION REWRITE] table [%s] output=%s inputs=%d records=%d bytes=%,d *****",
                tableName, relativeOutputPath, bin.size(), records, size));

        return new DataFile(relativeOutputPath, records, size, columnStats, logicalSchema.getSchemaId());
    }

    private void cleanupCreatedOutputs(String tableName, List<String> createdKeys) {
        if (createdKeys.isEmpty()) {
            return;
        }
        try {
            s3RetryExecutor.execute(() -> {
                List<ObjectIdentifier> objects = createdKeys.stream()
                        .map(key -> ObjectIdentifier.builder().key(key).build())
                        .toList();
                s3Client.deleteObjects(DeleteObjectsRequest.builder()
                        .bucket(bucketName)
                        .delete(Delete.builder().objects(objects).build())
                        .build());
                return null;
            }, "S3-DeleteObjects-compaction-cleanup-" + tableName);
            LOGGER.info(String.format(
                    "**** [COMPACTION CLEANUP] table [%s] deleted %d unreferenced output file(s) after rewrite failure *****",
                    tableName, createdKeys.size()));
        } catch (Exception cleanupError) {
            LOGGER.log(Level.WARNING, "Failed to clean up partial compaction outputs for table " + tableName,
                    cleanupError);
        }
    }

    /**
     * Resolves the snapshot referenced by {@code current-snapshot-id}. Kept local to the
     * compaction module so Module 1's catalog model stays untouched.
     */
    private static Snapshot findActiveSnapshot(TableMetadata metadata) {
        Long currentSnapshotId = metadata.getCurrentSnapshotId();
        if (currentSnapshotId == null || metadata.getSnapshots() == null) {
            return null;
        }
        return metadata.getSnapshots().stream()
                .filter(s -> s.getSnapshotId() == currentSnapshotId)
                .findFirst()
                .orElseThrow(() -> new IllegalStateException(
                        "Active snapshot ID " + currentSnapshotId + " not found."));
    }

    private Path s3aPath(String tableName, String relativePath) {
        return new Path("s3a://" + bucketName + "/" + tableKey(tableName, relativePath));
    }

    private static String tableKey(String tableName, String relativePath) {
        return "tables/" + tableName + "/" + relativePath;
    }

    private static String outputPath(String partition, String runId, int index) {
        String fileName = String.format("part-compacted-%s-%03d.parquet", runId, index);
        return partition == null || partition.isEmpty() ? fileName : partition + "/" + fileName;
    }

    /**
     * Outcome of one compaction pass. Before/after byte metrics are reported separately for the
     * active snapshot (the query-time manifest) and the full metadata JSON (which retains history
     * and normally grows).
     */
    public record Result(
            boolean committed,
            int newMetadataVersion,
            Long parentSnapshotId,
            int filesBefore,
            int filesAfter,
            long activeSnapshotBytesBefore,
            long activeSnapshotBytesAfter,
            long metadataJsonBytesBefore,
            long metadataJsonBytesAfter,
            int partitionsCompacted,
            int inputFilesConsumed,
            int outputFilesWritten,
            long inputBytes,
            long outputBytes,
            long recordsRewritten,
            List<DataFile> compactedFiles) {

        public Result {
            compactedFiles = List.copyOf(compactedFiles);
        }
    }
}
