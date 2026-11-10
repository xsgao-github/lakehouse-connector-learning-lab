# Module 06: Columnar Projection and Predicate Pushdown

## Overview
Module 06 establishes the core execution-layer scan pipeline for the Lakehouse connector. It enables efficient analytical queries by combining **physical columnar projection pushdown** at the Parquet storage layer with **row-level predicate filtering** (`QueryPredicate`), significantly minimizing network I/O over S3/MinIO and memory footprint.

## Technical Mechanism: How Projection Pushdown Works in Code

### 1. Parquet Reader Pushdown (`ParquetTableReader.java`)
To prevent Hadoop and S3A from fetching unrequested column bytes over the network, `ParquetTableReader` derives column names from `projectedFields`, reads the file metadata footer, extracts the native `MessageType`, and filters fields down to the requested projection. Setting `ReadSupport.PARQUET_READ_SCHEMA` instructs Parquet to issue selective byte-range S3 requests only for relevant column chunks.

```java
public class ParquetTableReader implements RowReader {
    public ParquetTableReader(Schema schema, List<Field> projectedFields, Path path, S3Client s3Client) throws IOException {
        List<String> projectedColumnNames = projectedFields == null ? null : projectedFields.stream()
                .map(Field::getName)
                .toList();
        Configuration conf = new Configuration();

        // Step A: Fetch file footer metadata to inspect actual column definitions
        ParquetMetadata metadata = ParquetFileReader.readFooter(conf, path, ParquetMetadataConverter.NO_FILTER);
        MessageType fileSchema = metadata.getFileMetaData().getSchema();

        // Step B: Filter file schema fields down to projected columns
        List<Type> projectedTypes = fileSchema.getFields().stream()
                .filter(field -> projectedColumnNames == null || projectedColumnNames.contains(field.getName()))
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

### 2. Execution-Layer Streaming & Filtering (`TableScanOperator.java` & `FilterOperator.java`)
The pipeline communicates over standard Java `Iterator<RecordBatch>` streams: `ParquetTableReader` implements `RowReader` for storage I/O and projection pushdown, `TableScanOperator` converts the injected reader's rows into record batches, and `FilterOperator` evaluates `QueryPredicate` filters on surviving rows.

```java
public class FilterOperator implements Iterator<RecordBatch> {
    private final Iterator<RecordBatch> upstream;
    private final QueryPredicate predicate;
    private RecordBatch nextFilteredBatch = null;

    public FilterOperator(Iterator<RecordBatch> upstream, QueryPredicate predicate) {
        this.upstream = upstream;
        this.predicate = predicate;
        advance();
    }

    private void advance() {
        nextFilteredBatch = null;
        while (upstream.hasNext()) {
            RecordBatch rawBatch = upstream.next();
            RecordBatch filteredBatch = filterBatch(rawBatch);

            if (filteredBatch.getRowCount() > 0) {
                nextFilteredBatch = filteredBatch;
                break;
            }
        }
    }

    private RecordBatch filterBatch(RecordBatch batch) {
        RecordBatch filtered = new RecordBatch(batch.getFields());
        for (int i = 0; i < batch.getRowCount(); i++) {
            Row row = batch.getRow(i);
            if (predicate == null || predicate.evaluate(row)) {
                filtered.addRow(row);
            }
        }
        return filtered;
    }

    @Override
    public boolean hasNext() {
        return nextFilteredBatch != null;
    }

    @Override
    public RecordBatch next() {
        if (!hasNext()) {
            throw new NoSuchElementException("No more matching records in filter operator.");
        }
        RecordBatch current = nextFilteredBatch;
        advance();
        return current;
    }
}
```

## Solved Challenges: Preventing `InvalidRecordException`

When applying schema projection, Parquet strictly matches requested fields against the file footer. Mismatches between `required` and `optional` column modifiers cause `org.apache.parquet.io.InvalidRecordException`.

**Solution:** Dynamically reading the file's native `MessageType` from the footer and filtering its existing `Type` definitions guarantees 100% attribute and repetition type alignment during reader initialization.