package xsgao.lakehouseconnector.query;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3Client;
import xsgao.lakehouseconnector.catalog.DataFile;
import xsgao.lakehouseconnector.catalog.Field;
import xsgao.lakehouseconnector.catalog.Schema;
import xsgao.lakehouseconnector.model.RecordBatch;
import xsgao.lakehouseconnector.model.Row;
import xsgao.lakehouseconnector.parquet.ParquetTableReader;
import xsgao.lakehouseconnector.parquet.ParquetTableWriter;
import xsgao.lakehouseconnector.planner.TableSplit;
import xsgao.lakehouseconnector.reader.RowReader;
import xsgao.lakehouseconnector.utils.S3ClientFactory;
import xsgao.lakehouseconnector.utils.S3ClientUtils;
import xsgao.lakehouseconnector.writer.TableWriterService;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static xsgao.lakehouseconnector.utils.S3ClientUtils.BUCKET_NAME;

public class QueryExecutorTest {    private S3Client s3Client;    private String tableName;
    private Schema schema;

    @BeforeEach
    public void setUp() {
        s3Client = S3ClientFactory.createDefaultClient();
        S3ClientUtils.ensureBucketExists(s3Client, BUCKET_NAME);
        tableName = "query_executor_test_" + System.currentTimeMillis();
        schema = new Schema(1, List.of(
            new Field(1, "id", "integer", false),
            new Field(2, "name", "string", false)
        ));
    }

    @AfterEach
    void tearDown() {
        if ("true".equals(System.getProperty("keepData"))) {
            return;
        }
        S3ClientUtils.deleteTable(s3Client, BUCKET_NAME, tableName);
    }

    @Test
    public void testExecuteScansAllRowsFromEverySplitInMetadata() throws Exception {
        // 1. Write two Parquet data files, one per planned split
        String split1Path = "data/dt=2026-11-11/part-001.parquet";
        String split2Path = "data/dt=2026-11-11/part-002.parquet";
        writeDataFile(split1Path, List.of(record(1, "Alice"), record(2, "Bob")));
        writeDataFile(split2Path, List.of(record(3, "Carol"), record(4, "Dave")));

        // 2. Commit a single metadata version referencing both splits
        TableWriterService writerService = new TableWriterService(s3Client, BUCKET_NAME, 3, 50);
        writerService.commitTableTransaction(tableName, schema, List.of(
            new DataFile(split1Path, 2, 0L),
            new DataFile(split2Path, 2, 0L)
        ), null);

        // 3. Execute through QueryExecutor, opening one Parquet reader per split
        QueryExecutor queryExecutor = new QueryExecutor(s3Client, BUCKET_NAME, 2);
        List<RecordBatch> batches = queryExecutor.execute(tableName, null, null, this::openReader);

        // 4. Both splits' rows must be returned, in split order
        List<Row> rows = batches.stream()
            .flatMap(batch -> batch.getRows().stream())
            .toList();

        assertEquals(4, rows.size(), "Both splits should contribute all their rows");
        assertEquals(List.of(1, 2, 3, 4), rows.stream().map(row -> row.getValue("id")).toList());
        assertEquals(List.of("Alice", "Bob", "Carol", "Dave"),
            rows.stream().map(row -> row.getValue("name")).toList());
    }

    private void writeDataFile(String filePath, List<Map<String, Object>> records) throws IOException {
        Path path = new Path("s3a://" + BUCKET_NAME + "/tables/" + tableName + "/" + filePath);
        try (ParquetTableWriter writer = new ParquetTableWriter(schema, path, 1024 * 1024, CompressionCodecName.SNAPPY)) {
            for (Map<String, Object> record : records) {
                writer.writeRecord(record);
            }
        }
    }

    private RowReader openReader(TableSplit split) {
        try {
            Path path = new Path("s3a://" + BUCKET_NAME + "/tables/" + tableName + "/" + split.getFilePath());
            return new ParquetTableReader(schema, null, path);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to open reader for split: " + split.getFilePath(), e);
        }
    }

    private static Map<String, Object> record(int id, String name) {
        Map<String, Object> row = new HashMap<>();
        row.put("id", id);
        row.put("name", name);
        return row;
    }
}
