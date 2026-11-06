package xsgao.lakehouseconnector.parquet;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;

import software.amazon.awssdk.services.s3.S3Client;
import xsgao.lakehouseconnector.catalog.ColumnStats;
import xsgao.lakehouseconnector.catalog.Field;
import xsgao.lakehouseconnector.catalog.Schema;
import static xsgao.lakehouseconnector.utils.S3ClientFactory.MINIO_ACCESS_KEY;
import static xsgao.lakehouseconnector.utils.S3ClientFactory.MINIO_SECRET_KEY;

public class ParquetTableReader implements AutoCloseable {
    private final Schema schema;
    private final Schema projectedSchema;
    private final ParquetReader<Group> reader;
    private final Configuration conf;
    private final S3Client s3Client;

    public ParquetTableReader(Schema schema, List<String> projectedColumns, Path path, S3Client s3Client) throws IOException {
        this.schema = schema;
        this.projectedSchema = new Schema(schema.getSchemaId(), schema.getFields().stream()
                .filter(f -> projectedColumns.contains(f.getName()))
                .toList());
        this.s3Client = s3Client;
        this.conf = new Configuration();
        
        Configuration conf = new Configuration();
        conf.set("fs.s3a.path.style.access", "true"); // Set to true for MinIO and some S3-compatible services
        conf.set("fs.s3a.connection.ssl.enabled", "false");
        conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
        conf.set("fs.s3a.access.key", MINIO_ACCESS_KEY);
        conf.set("fs.s3a.secret.key", MINIO_SECRET_KEY);

        if (s3Client != null) {
            s3Client.serviceClientConfiguration().endpointOverride()
                    .ifPresent(endpoint -> conf.set("fs.s3a.endpoint", endpoint.toString()));
        }

        this.reader = ParquetReader.builder(new GroupReadSupport(), path)
                .withConf(conf)
                .build();
    }

    /**
     * Reads the next record from the Parquet file and maps its column values 
     * into a Java Map based on the defined schema.
     * 
     * <p>Note: This method consumes a single {@link org.apache.parquet.example.data.Group} 
     * returned row-by-row from the underlying {@link org.apache.parquet.hadoop.ParquetReader}. 
     * For high-throughput analytical scans, production engines bypass this row-by-row 
     * Group API in favor of vectorized batch readers (e.g., Apache Arrow) to eliminate 
     * per-row object allocation overhead.
     * 
     * @return A map of column names to values representing the next record, 
     *         or {@code null} if the end of the file has been reached.
     * @throws IOException if an error occurs while reading from the underlying stream.
     */
    public Map<String, Object> readNextRecord() throws IOException {
        Group group = reader.read();
        if (group == null) {
            return null;
        }

        Map<String, Object> record = new HashMap<>();
        for (Field field : schema.getFields()) {
            String colName = field.getName();
            try {
                // Check if field exists in this specific group instance
                int fieldIndex = group.getType().getFieldIndex(colName);
                if (fieldIndex >= 0 && group.getFieldRepetitionCount(fieldIndex) > 0) {
                    Object val = extractValueFromGroup(group, colName, field.getType());
                    if (val != null) {
                        record.put(colName, val);
                    }
                }
            } catch (IllegalArgumentException e) {
                // Column not present in file schema projection
            }
        }
        return record;
    }
    public Map<String, ColumnStats> extractFooterStatistics(Path path) throws IOException {
        ParquetMetadata metadata = ParquetFileReader.readFooter(conf, path, ParquetMetadataConverter.NO_FILTER);
        Map<String, ColumnStats> statsMap = new HashMap<>();

        for (BlockMetaData block : metadata.getBlocks()) {
            for (ColumnChunkMetaData chunk : block.getColumns()) {
                String colName = chunk.getPath().toDotString();
                Statistics<?> parquetStats = chunk.getStatistics();

                if (parquetStats != null && !parquetStats.isEmpty()) {
                    Object minVal = extractMinStat(parquetStats);
                    Object maxVal = extractMaxStat(parquetStats);
                    long nullCount = parquetStats.getNumNulls();

                    statsMap.putIfAbsent(colName, new ColumnStats(String.valueOf(minVal), String.valueOf(maxVal), nullCount));
                }
            }
        }
        return statsMap;
    }

    private Object extractMinStat(Statistics<?> stats) {
        if (!stats.hasNonNullValue()) return null;
        return stats.genericGetMin();
    }

    private Object extractMaxStat(Statistics<?> stats) {
        if (!stats.hasNonNullValue()) return null;
        return stats.genericGetMax();
    }

    private Object extractValueFromGroup(Group group, String colName, String typeStr) {
        if (typeStr == null) return group.getString(colName, 0);

        switch (typeStr.toLowerCase().trim()) {
            case "int": case "integer": case "int32":
                try {
                    return group.getInteger(colName, 0);
                } catch (ClassCastException e) {
                    return (int) group.getLong(colName, 0);
                }
            case "long": case "bigint": case "int64": case "date": return group.getLong(colName, 0);
            case "float": return group.getFloat(colName, 0);
            case "double": case "decimal": return group.getDouble(colName, 0);
            case "boolean": return group.getBoolean(colName, 0);
            default: return group.getString(colName, 0);
        }
    }

    @Override
    public void close() throws IOException {
        if (reader != null) {
            reader.close();
        }
    }
}