# Module 06: Columnar Projection and Predicate Pushdown

## Overview
Module 06 establishes the core execution-layer scan pipeline for the Lakehouse connector. It enables efficient analytical queries by combining **physical columnar projection pushdown** at the Parquet storage layer with **row-level predicate filtering** (`QueryPredicate`), significantly minimizing network I/O over S3/MinIO and memory footprint.

## Technical Mechanism: How Projection Pushdown Works in Code

### 1. Parquet Reader Pushdown (`ParquetTableReader.java`)
To prevent Hadoop and S3A from fetching unrequested column bytes over the network, `ParquetTableReader` reads the file metadata footer, extracts the native `MessageType`, and filters fields down to `projectedColumns`. Setting `ReadSupport.PARQUET_READ_SCHEMA` instructs Parquet to issue selective byte-range S3 requests only for relevant column chunks.

```java
public class ParquetTableReader implements AutoCloseable {
    public ParquetTableReader(Schema schema, List<String> projectedColumns, Path path, S3Client s3Client) throws IOException {
        Configuration conf = new Configuration();

        // Step A: Fetch file footer metadata to inspect actual column definitions
        ParquetMetadata metadata = ParquetFileReader.readFooter(conf, path, ParquetMetadataConverter.NO_FILTER);
        MessageType fileSchema = metadata.getFileMetaData().getSchema();

        // Step B: Filter file schema fields down to projected columns
        List<Type> projectedTypes = fileSchema.getFields().stream()
                .filter(field -> projectedColumns == null || projectedColumns.contains(field.getName()))
                .toList();

        MessageType requestedProjection = new MessageType(fileSchema.getName(), projectedTypes);

        // Step C: Pushdown the projected MessageType to Hadoop read configuration
        // This forces Parquet to skip unrequested S3/MinIO byte ranges entirely
        conf.set(org.apache.parquet.hadoop.api.ReadSupport.PARQUET_READ_SCHEMA, requestedProjection.toString());

        this.reader = ParquetReader.builder(new GroupReadSupport(), path)
                .withConf(conf)
                .build();
    }
}
```

### 2. Execution-Layer Streaming & Filtering (`ProjectedScanReader.java`)
`ProjectedScanReader` acts as an iterator over `RecordBatch` streams, evaluating `QueryPredicate` filters on surviving rows.

```java
public class ProjectedScanReader implements Iterator<RecordBatch> {
    private void advance() {
        while (rawBatchIterator.hasNext()) {
            RecordBatch rawBatch = rawBatchIterator.next();
            RecordBatch filteredBatch = filterBatch(rawBatch);
            
            // Skip empty batches post-filtering
            if (filteredBatch.getRowCount() > 0) {
                this.nextFilteredBatch = filteredBatch;
                return;
            }
        }
        this.nextFilteredBatch = null;
    }

    private RecordBatch filterBatch(RecordBatch batch) {
        List<Row> matchingRows = new ArrayList<>();
        for (Row row : batch.getRows()) {
            if (predicate == null || predicate.evaluate(row)) {
                matchingRows.add(row);
            }
        }
        return new RecordBatch(batch.getFields(), matchingRows);
    }
}
```

## Solved Challenges: Preventing `InvalidRecordException`

When applying schema projection, Parquet strictly matches requested fields against the file footer. Mismatches between `required` and `optional` column modifiers cause `org.apache.parquet.io.InvalidRecordException`.

**Solution:** Dynamically reading the file's native `MessageType` from the footer and filtering its existing `Type` definitions guarantees 100% attribute and repetition type alignment during reader initialization.
