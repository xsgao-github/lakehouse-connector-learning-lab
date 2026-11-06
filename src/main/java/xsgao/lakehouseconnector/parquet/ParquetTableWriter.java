package xsgao.lakehouseconnector.parquet;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;

import software.amazon.awssdk.services.s3.S3Client;
import xsgao.lakehouseconnector.catalog.Field;
import xsgao.lakehouseconnector.catalog.Schema;
import static xsgao.lakehouseconnector.utils.S3ClientFactory.MINIO_ACCESS_KEY;
import static xsgao.lakehouseconnector.utils.S3ClientFactory.MINIO_SECRET_KEY;

public class ParquetTableWriter implements AutoCloseable {
    private final Schema schema;
    private final MessageType parquetSchema;
    private final ParquetWriter<Group> writer;
    private final SimpleGroupFactory groupFactory;
    private final S3Client s3Client;

    public ParquetTableWriter(Schema schema, Path outputPath, int rowGroupSize, CompressionCodecName codec, S3Client s3Client) throws IOException {
        this.schema = schema;
        this.parquetSchema = toParquetMessageType(schema);
        this.groupFactory = new SimpleGroupFactory(parquetSchema);
        this.s3Client = s3Client;

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



        this.writer = ExampleParquetWriter.builder(outputPath)
                .withType(parquetSchema)
                .withCompressionCodec(codec)
                .withRowGroupSize(rowGroupSize)
                .withPageSize(ParquetProperties.DEFAULT_PAGE_SIZE)
                .withConf(conf)
                .build();
    }

    /**
     * Writes a single record map into the Parquet file by mapping schema fields
     * to a newly allocated Parquet Group.
     * 
     * <p>Note: This method instantiates a new {@link org.apache.parquet.example.data.Group} 
     * per record via {@code groupFactory.newGroup()}. While simple and clean for learning labs 
     * and low-to-medium throughput, high-throughput production connectors should eventually 
     * bypass the Group API to minimize per-row object allocation and GC pressure.
     * 
     * @param recordData A map containing column names as keys and record values as values.
     * @throws IOException if an error occurs during serialization or writing to the underlying stream.
     */
    public void writeRecord(Map<String, Object> recordData) throws IOException {
        Group group = groupFactory.newGroup();
        for (Field field : schema.getFields()) {
            String colName = field.getName();
            Object val = recordData.get(colName);
            if (val != null) {
                appendValueToGroup(group, colName, val, field.getType());
            }
        }
        writer.write(group);
    }

    private void appendValueToGroup(Group group, String colName, Object val, String typeStr) {
        if (typeStr == null) {
            group.add(colName, val.toString());
            return;
        }

        switch (typeStr.toLowerCase().trim()) {
            case "int":
            case "integer":
            case "int32":
                group.add(colName, ((Number) val).intValue());
                break;
            case "long":
            case "bigint":
            case "int64":
            case "date":
                group.add(colName, ((Number) val).longValue());
                break;
            case "float":
                group.add(colName, ((Number) val).floatValue());
                break;
            case "double":
            case "decimal":
                group.add(colName, ((Number) val).doubleValue());
                break;
            case "boolean":
                group.add(colName, (Boolean) val);
                break;
            case "string":
            case "varchar":
            case "char":
            default:
                group.add(colName, val.toString());
                break;
        }
    }

    private MessageType toParquetMessageType(Schema schema) {
        Types.MessageTypeBuilder builder = Types.buildMessage();
        List<Field> fields = schema.getFields();
        for (Field field : fields) {
            PrimitiveType.PrimitiveTypeName parquetType = mapDataType(field.getType());
            if (field.isRequired()) {
                builder.required(parquetType).named(field.getName());
            } else {
                builder.optional(parquetType).named(field.getName());
            }
        }
        return builder.named("LakehouseTableRecord");
    }

    private PrimitiveType.PrimitiveTypeName mapDataType(String typeStr) {
        if (typeStr == null) return PrimitiveType.PrimitiveTypeName.BINARY;
        switch (typeStr.toLowerCase().trim()) {
            case "int": case "integer": case "int32": return PrimitiveType.PrimitiveTypeName.INT32;
            case "long": case "bigint": case "int64": case "date": return PrimitiveType.PrimitiveTypeName.INT64;
            case "float": return PrimitiveType.PrimitiveTypeName.FLOAT;
            case "double": case "decimal": return PrimitiveType.PrimitiveTypeName.DOUBLE;
            case "boolean": return PrimitiveType.PrimitiveTypeName.BOOLEAN;
            default: return PrimitiveType.PrimitiveTypeName.BINARY;
        }
    }

    @Override
    public void close() throws IOException {
        if (writer != null) {
            writer.close();
        }
    }
}