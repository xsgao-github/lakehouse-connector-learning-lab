package xsgao.lakehouseconnector.parquet;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

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
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import xsgao.lakehouseconnector.catalog.ColumnStats;
import xsgao.lakehouseconnector.catalog.Field;
import xsgao.lakehouseconnector.catalog.Schema;
import xsgao.lakehouseconnector.reader.RowReader;

public class ParquetTableReader implements RowReader {

    private static final Logger LOGGER = Logger.getLogger(ParquetTableReader.class.getName());

    private final Schema projectedSchema;
    private final Map<Integer, Field> fileFieldsById;
    private final Map<Integer, String> logicalToFileNames;
    private final ParquetReader<Group> reader;
    private final Configuration conf;

    public ParquetTableReader(Schema schema, List<Field> projectedFields, Path path) throws IOException {
        this(schema, schema, projectedFields, path);
    }

    /**
     * Reads a file written under {@code fileSchema} and exposes it through
     * {@code logicalSchema}. Columns are matched by immutable field id, so renamed columns
     * keep their values and dropped columns are ignored.
     *
     * @param fileSchema    schema the file was written with (resolved via its schema-id)
     * @param logicalSchema current table schema whose names appear in returned records
     */
    public ParquetTableReader(Schema fileSchema, Schema logicalSchema, List<Field> projectedFields, Path path)
            throws IOException {
        Schema effectiveFileSchema = fileSchema == null ? logicalSchema : fileSchema;
        List<Field> logicalFields = projectedFields == null ? logicalSchema.getFields() : projectedFields;
        this.projectedSchema = new Schema(logicalSchema.getSchemaId(), logicalFields);

        this.fileFieldsById = new HashMap<>();
        for (Field field : effectiveFileSchema.getFields()) {
            fileFieldsById.putIfAbsent(field.getId(), field);
        }

        this.logicalToFileNames = new HashMap<>();
        for (Field logical : logicalFields) {
            Field fileField = fileFieldsById.get(logical.getId());
            if (fileField != null) {
                logicalToFileNames.put(logical.getId(), fileField.getName());
            }
        }

        this.conf = S3AConfig.create();

        // 1. Read file metadata footer to obtain the exact file schema written by ParquetTableWriter
        ParquetMetadata metadata = ParquetFileReader.readFooter(conf, path, ParquetMetadataConverter.NO_FILTER);
        MessageType fileSchemaFromFooter = metadata.getFileMetaData().getSchema();

        // 2. Map each logical field to the physical name present in this file. Legacy files
        //    without schema history fall back to name matching.
        Set<String> requestedNames = new HashSet<>();
        for (Field logical : logicalFields) {
            String fileSideName = logicalToFileNames.get(logical.getId());
            if (fileSideName == null && fileSchemaFromFooter.containsField(logical.getName())) {
                fileSideName = logical.getName();
                logicalToFileNames.put(logical.getId(), fileSideName);
            }
            if (fileSideName != null && fileSchemaFromFooter.containsField(fileSideName)) {
                requestedNames.add(fileSideName);
            }
        }

        // 3. Filter the footer schema to the requested physical columns
        List<Type> projectedTypes = fileSchemaFromFooter.getFields().stream()
                .filter(field -> requestedNames.contains(field.getName()))
                .toList();

        MessageType requestedProjection = new MessageType(fileSchemaFromFooter.getName(), projectedTypes);

        // 4. Set requested projection in Hadoop configuration using the exact file-derived types
        conf.set(org.apache.parquet.hadoop.api.ReadSupport.PARQUET_READ_SCHEMA, requestedProjection.toString());

        this.reader = ParquetReader.builder(new GroupReadSupport(), path)
                .withConf(conf)
                .build();
    }

    @Override
    public List<Field> getFields() {
        return projectedSchema.getFields();
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
    @Override
    public Map<String, Object> readNextRecord() throws IOException {
        Group group = reader.read();

        if (group != null) {
            // This returns the exact number of columns loaded by the physical Parquet reader
            int physicalColumnCount = group.getType().getFieldCount();
            LOGGER.info("**** [PARQUET READ] Read record with " + physicalColumnCount + " physical columns from Parquet file. *****");
        }

        if (group == null) {
            return null;
        }

        Map<String, Object> record = new HashMap<>();
        for (Field logical : projectedSchema.getFields()) {
            String fileSideName = logicalToFileNames.getOrDefault(logical.getId(), logical.getName());
            if (!group.getType().containsField(fileSideName)) {
                // Column absent from this file (additive schema evolution): leave it null.
                continue;
            }
            int fieldIndex = group.getType().getFieldIndex(fileSideName);
            if (fieldIndex >= 0 && group.getFieldRepetitionCount(fieldIndex) > 0) {
                Field fileField = fileFieldsById.get(logical.getId());
                String fileType = fileField != null ? fileField.getType() : logical.getType();
                Object val = extractValueFromGroup(group, fileSideName, fileType);
                if (val != null) {
                    record.put(logical.getName(), coerce(val, logical.getType()));
                }
            }
        }
        return record;
    }

    public Map<String, ColumnStats> extractFooterStatistics(Path path) throws IOException {
        ParquetMetadata metadata = ParquetFileReader.readFooter(conf, path, ParquetMetadataConverter.NO_FILTER);
        Map<String, ColumnStats> statsMap = new HashMap<>();
        Map<String, String> fileToLogical = new HashMap<>();
        for (Field logical : projectedSchema.getFields()) {
            fileToLogical.putIfAbsent(
                    logicalToFileNames.getOrDefault(logical.getId(), logical.getName()),
                    logical.getName());
        }

        for (BlockMetaData block : metadata.getBlocks()) {
            for (ColumnChunkMetaData chunk : block.getColumns()) {
                String colName = chunk.getPath().toDotString();
                Statistics<?> parquetStats = chunk.getStatistics();

                if (parquetStats != null && !parquetStats.isEmpty()) {
                    Object minVal = extractMinStat(parquetStats);
                    Object maxVal = extractMaxStat(parquetStats);
                    long nullCount = parquetStats.getNumNulls();

                    String logicalName = fileToLogical.getOrDefault(colName, colName);
                    statsMap.putIfAbsent(logicalName, new ColumnStats(String.valueOf(minVal), String.valueOf(maxVal), nullCount));
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
            case "int": case "integer": case "int32": return group.getInteger(colName, 0);
            case "long": case "bigint": case "int64": case "date": return group.getLong(colName, 0);
            case "float": return group.getFloat(colName, 0);
            case "double": case "decimal": return group.getDouble(colName, 0);
            case "boolean": return group.getBoolean(colName, 0);
            default: return group.getString(colName, 0);
        }
    }

    /**
     * Coerces the physical value read from the file into the current logical type. This keeps
     * simple type promotions (int -> long, int -> double, anything -> string) readable.
     */
    private static Object coerce(Object value, String logicalType) {
        if (value == null || logicalType == null) {
            return value;
        }
        String type = logicalType.toLowerCase().trim();
        try {
            switch (type) {
                case "int":
                case "integer":
                case "int32":
                    return ((Number) value).intValue();
                case "long":
                case "bigint":
                case "int64":
                case "date":
                    return ((Number) value).longValue();
                case "float":
                    return ((Number) value).floatValue();
                case "double":
                case "decimal":
                    return ((Number) value).doubleValue();
                case "boolean":
                    return value instanceof Boolean ? value : Boolean.parseBoolean(value.toString());
                default:
                    return value.toString();
            }
        } catch (ClassCastException | NumberFormatException e) {
            return value;
        }
    }

    @Override
    public void close() throws IOException {
        if (reader != null) {
            reader.close();
        }
    }
}