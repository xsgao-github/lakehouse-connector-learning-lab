package xsgao.lakehouseconnector.compaction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import software.amazon.awssdk.services.s3.S3Client;

import xsgao.lakehouseconnector.catalog.CatalogService;
import xsgao.lakehouseconnector.catalog.ColumnStats;
import xsgao.lakehouseconnector.catalog.DataFile;
import xsgao.lakehouseconnector.catalog.Field;
import xsgao.lakehouseconnector.catalog.Schema;
import xsgao.lakehouseconnector.catalog.Snapshot;
import xsgao.lakehouseconnector.catalog.TableMetadata;
import xsgao.lakehouseconnector.model.RecordBatch;
import xsgao.lakehouseconnector.model.Row;
import xsgao.lakehouseconnector.parquet.ParquetTableReader;
import xsgao.lakehouseconnector.parquet.ParquetTableWriter;
import xsgao.lakehouseconnector.planner.SplitPlanner;
import xsgao.lakehouseconnector.planner.TableSplit;
import xsgao.lakehouseconnector.query.QueryExecutor;
import xsgao.lakehouseconnector.reader.RowReader;
import xsgao.lakehouseconnector.utils.S3ClientFactory;
import xsgao.lakehouseconnector.utils.S3ClientUtils;
import xsgao.lakehouseconnector.writer.TableWriterService;

import static org.assertj.core.api.Assertions.assertThat;
import static xsgao.lakehouseconnector.utils.S3ClientUtils.BUCKET_NAME;

class CompactionServiceTest {

    private S3Client s3Client;
    private CatalogService catalogService;
    private CompactionService compactionService;
    private String tableName;
    private Schema schema;

    @BeforeEach
    void setUp() {
        s3Client = S3ClientFactory.createDefaultClient();
        S3ClientUtils.ensureBucketExists(s3Client, BUCKET_NAME);

        tableName = "compaction_service_test_" + System.currentTimeMillis();
        schema = new Schema(1, List.of(
                new Field(1, "id", "integer", true),
                new Field(2, "name", "string", false),
                new Field(3, "dt", "string", true)));

        catalogService = new CatalogService(s3Client, BUCKET_NAME);
        compactionService = new CompactionService(s3Client, BUCKET_NAME);
    }

    @AfterEach
    void tearDown() {
        if ("true".equals(System.getProperty("keepData"))) {
            return;
        }
        S3ClientUtils.deleteTable(s3Client, BUCKET_NAME, tableName);
    }

    @Test
    void compactsSmallFilesAfterSchemaEvolutionIntoOneFileReadersUse() throws Exception {
        // v1 schema: id, name, dt. These files predate the nullable "region" column.
        List<DataFile> smallFiles = new ArrayList<>();
        int id = 1;
        for (int i = 1; i <= 4; i++) {
            smallFiles.add(writeDataFile(
                    String.format("data/dt=2026-11-12/part-%03d.parquet", i),
                    List.of(record(id++, "a" + i, "2026-11-12"),
                            record(id++, "b" + i, "2026-11-12"))));
        }
        assertThat(commit(smallFiles, null)).isEqualTo(1);
        long v1SnapshotId = catalogService.loadTableMetadata(tableName).getCurrentSnapshotId();

        // Evolve the metadata schema: rename field id 2 and add a nullable column.
        // Existing files keep field id 2 under its old name; only new writes use full_name.
        schema = new Schema(2, List.of(
                new Field(1, "id", "integer", true),
                new Field(2, "full_name", "string", false),
                new Field(3, "dt", "string", true),
                new Field(4, "region", "string", false)));

        List<DataFile> liveFiles = new ArrayList<>(smallFiles);
        liveFiles.add(writeDataFile("data/dt=2026-11-12/part-005.parquet",
                List.of(recordWithRegion(9, "i", "2026-11-12", "US"),
                        recordWithRegion(10, "j", "2026-11-12", "EU"))));
        assertThat(commit(liveFiles, v1SnapshotId)).isEqualTo(2);

        CompactionOptions options = new CompactionOptions(
                1_000_000L, 3, 1_000_000L, 1024 * 1024, CompressionCodecName.SNAPPY);
        CompactionService.Result result = compactionService.compactTable(tableName, options);

        assertThat(result.committed()).isTrue();
        assertThat(result.newMetadataVersion()).isEqualTo(3);
        assertThat(result.filesBefore()).isEqualTo(5);
        assertThat(result.filesAfter()).isEqualTo(1);
        assertThat(result.outputFilesWritten()).isEqualTo(1);
        assertThat(result.recordsRewritten()).isEqualTo(10);

        TableMetadata v3 = catalogService.loadTableMetadata(tableName, 3);
        Snapshot activeSnapshot = activeSnapshot(v3);
        assertThat(activeSnapshot.getAddedFiles()).hasSize(1);
        assertThat(activeSnapshot.getRemovedFiles()).hasSize(5);
        assertThat(activeSnapshot.getSchemaId()).isEqualTo(2L);

        DataFile compacted = activeSnapshot.getAddedFiles().get(0);
        assertThat(compacted.getFilePath())
                .startsWith("data/dt=2026-11-12/part-compacted-")
                .endsWith(".parquet");
        assertThat(compacted.getRecordCount()).isEqualTo(10);
        assertThat(compacted.getSchemaId()).isEqualTo(2L);
        assertThat(compacted.getColumnStats()).containsKeys("full_name", "region");

        // The planner and the reader now see exactly one live file.
        assertThat(new SplitPlanner().planSplits(v3, null, null))
                .extracting(TableSplit::getFilePath)
                .containsExactly(compacted.getFilePath());

        QueryExecutor queryExecutor = new QueryExecutor(s3Client, BUCKET_NAME, 2);
        List<RecordBatch> batches = queryExecutor.execute(tableName, null, null, this::openReader);
        List<Row> rows = batches.stream()
                .flatMap(batch -> batch.getRows().stream())
                .toList();

        assertThat(rows).hasSize(10);
        assertThat(rows.stream().map(row -> ((Number) row.getValue("id")).intValue()).toList())
                .containsExactlyInAnyOrder(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        // Old rows were written before the evolution: field id 2 is renamed to full_name and
        // the new region column reads as null. New rows keep their values.
        for (Row row : rows) {
            int rowId = ((Number) row.getValue("id")).intValue();
            if (rowId <= 8) {
                assertThat(row.getValue("region")).as("old row %d", rowId).isNull();
                assertThat(row.getValue("full_name")).as("old row %d", rowId).isNotNull();
                assertThat(row.getValue("name")).as("renamed old row %d", rowId).isNull();
            } else if (rowId == 9) {
                assertThat(row.getValue("full_name")).isEqualTo("i");
                assertThat(row.getValue("region")).isEqualTo("US");
            } else {
                assertThat(row.getValue("full_name")).isEqualTo("j");
                assertThat(row.getValue("region")).isEqualTo("EU");
            }
        }
    }

    private DataFile writeDataFile(String relativePath, List<Map<String, Object>> records) throws Exception {
        Path path = s3aPath(relativePath);
        try (ParquetTableWriter writer = new ParquetTableWriter(
                schema, path, 1024 * 1024, CompressionCodecName.SNAPPY)) {
            for (Map<String, Object> record : records) {
                writer.writeRecord(record);
            }
        }

        String key = "tables/" + tableName + "/" + relativePath;
        long size = s3Client.headObject(b -> b.bucket(BUCKET_NAME).key(key)).contentLength();

        Map<String, ColumnStats> columnStats;
        try (ParquetTableReader reader = new ParquetTableReader(schema, null, path)) {
            columnStats = reader.extractFooterStatistics(path);
        }
        return new DataFile(relativePath, records.size(), size, columnStats);
    }

    private int commit(List<DataFile> files, Long parentSnapshotId) throws Exception {
        return new TableWriterService(s3Client, BUCKET_NAME, 3, 50)
                .commitTableTransaction(tableName, schema, files, parentSnapshotId);
    }

    private RowReader openReader(TableSplit split) {
        try {
            return new ParquetTableReader(schema, null, s3aPath(split.getFilePath()));
        } catch (IOException e) {
            throw new IllegalStateException("Failed to open reader for split: " + split.getFilePath(), e);
        }
    }

    private static Snapshot activeSnapshot(TableMetadata metadata) {
        Long currentSnapshotId = metadata.getCurrentSnapshotId();
        return metadata.getSnapshots().stream()
                .filter(s -> s.getSnapshotId() == currentSnapshotId)
                .findFirst()
                .orElseThrow();
    }

    private Path s3aPath(String relativePath) {
        return new Path("s3a://" + BUCKET_NAME + "/tables/" + tableName + "/" + relativePath);
    }

    private static Map<String, Object> record(int id, String name, String dt) {
        Map<String, Object> row = new HashMap<>();
        row.put("id", id);
        row.put("name", name);
        row.put("dt", dt);
        return row;
    }

    private static Map<String, Object> recordWithRegion(int id, String name, String dt, String region) {
        Map<String, Object> row = new HashMap<>();
        row.put("id", id);
        row.put("full_name", name);
        row.put("dt", dt);
        row.put("region", region);
        return row;
    }
}
