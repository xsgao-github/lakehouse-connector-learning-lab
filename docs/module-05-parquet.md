# Module 05: Parquet Table Writer & Reader

## Overview

Module 05 focuses on building a functional, S3-native Parquet I/O layer using Java 17 and Apache Hadoop/Parquet 3.4.0 libraries. This module establishes the core serialization and deserialization routines required to read and write self-describing columnar Parquet files with custom schema definitions.

## Key Technical Decisions & Architecture

### 1. Environment & Runtime Baseline
* **Platform Standard:** Development and testing are standardized on **Ubuntu/WSL2** using **JDK 17**.
* **Rationale:** Java 21+ and Java 25 introduce strict security checks around `Subject.getSubject()`, which cause runtime incompatibilities with Hadoop versions prior to 3.4.0. Standardizing on JDK 17 under Linux guarantees deterministic execution for S3A filesystem integration without native Windows binary (`winutils.exe`) dependencies.

### 2. Physical Layout & Sizing
* **Row Group Size:** Set to `1 MB` (`1024 * 1024` bytes) for small-scale lab testing. In production, row groups are tuned between `128 MB` and `512 MB` to balance network transfer efficiency and split granularity.
* **Compression Codec:** `SNAPPY` compression is applied by default across all column pages to achieve fast CPU decompression throughput.
* **File Finalization:** Writers implement `AutoCloseable`. Exiting a `try-with-resources` block triggers `writer.close()`, which flushes active memory buffers, appends column chunk statistics, writes the File Footer, and appends the final 4-byte `PAR1` magic header.

### 3. Record Processing & Memory Model
* **API Used:** Apache Parquet `Group` API (`SimpleGroupFactory` and `GroupWriteSupport`).
* **GC Impact & Trade-Off:** The current implementation instantiates a new `Group` object for every record written (`groupFactory.newGroup()`). While optimal for readability and educational purposes, this creates high Garbage Collection (GC) pressure at scale. High-throughput engines bypass the `Group` API in favor of low-level `RecordConsumer` or vectorized memory buffers (e.g., Apache Arrow).

## Core Implementations

### ParquetTableWriter
```java
public class ParquetTableWriter implements AutoCloseable {
    public ParquetTableWriter(Schema schema, Path path, long rowGroupSize, CompressionCodecName codec, Configuration conf) throws IOException {
        this.schema = schema;
        MessageType parquetSchema = SchemaConverter.toParquetSchema(schema);
        this.groupFactory = new SimpleGroupFactory(parquetSchema);

        GroupWriteSupport.setSchema(parquetSchema, conf);
        this.writer = ExampleParquetWriter.builder(path)
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .withCompressionCodec(codec)
                .withRowGroupSize(rowGroupSize)
                .withConf(conf)
                .build();
    }

    /**
     * Writes a single record map into the Parquet file by mapping schema fields
     * to a newly allocated Parquet Group.
     * 
     * Note: Allocates a new Group per record. Suitable for learning lab throughput, 
     * but production engines avoid per-row object allocations via direct RecordConsumer calls.
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
}
```

### ParquetTableReader
```java
public class ParquetTableReader implements AutoCloseable {
    public ParquetTableReader(Schema schema, Path path, Configuration conf) throws IOException {
        this.schema = schema;
        this.reader = ParquetReader.builder(new GroupReadSupport(), path)
                .withConf(conf)
                .build();
    }

    /**
     * Reads the next record from the Parquet file as a Java Map.
     * 
     * Reads records row-by-row sequentially. The reader automatically adapts to the 
     * physical row group configurations and compression codecs recorded in the file footer.
     */
    public Map<String, Object> readNextRecord() throws IOException {
        Group group = reader.read();
        if (group == null) {
            return null;
        }

        Map<String, Object> record = new HashMap<>();
        for (Field field : schema.getFields()) {
            String colName = field.getName();
            if (group.getFieldRepetitionCount(colName) > 0) {
                Object val = extractValueFromGroup(group, colName, field.getType());
                record.put(colName, val);
            }
        }
        return record;
    }
}
```


## Common Pitfalls & Solutions

### 1. JDK 21/25 Incompatibility with Hadoop 3.3.x (`Subject.getSubject()`)
* **Problem:** Running Hadoop clients on JDK 21+ throws `UnsupportedOperationException` or security manager runtime failures when Hadoop attempts user authentication checks via deprecated `Subject.getSubject()` APIs.
* **Solution:** Standardize the execution environment on JDK 17 or upgrade all Hadoop dependency artifacts to `3.4.0+`, where security context lookups have been refactored for modern Java runtimes.

### 2. Windows Native Binary (`winutils.exe` / `hadoop.dll`) Missing
* **Problem:** Executing Parquet files or Hadoop `LocalFileSystem` calls directly on native Windows leads to `UnsatisfiedLinkError` or `java.io.IOException: Cannot run program "winutils.exe"`.
* **Solution:** Develop and test inside **WSL2 (Ubuntu)**. This provides a native POSIX environment where Hadoop filesystem implementations run cleanly without extra Windows binary shims.

### 3. Unflushed Buffers & Missing Parquet Footers
* **Problem:** Readers fail with `Invalid Parquet file` or `Magic header missing` errors when attempting to read newly written files.
* **Solution:** Ensure all writer instances are wrapped inside `try-with-resources` blocks or explicitly call `writer.close()`. Parquet footers and the terminal `PAR1` magic bytes are only written during the final closing phase.

### 4. Per-Row Allocation Overhead (GC Pressure)
* **Problem:** Instantiating a new `Group` object per row using `groupFactory.newGroup()` creates excessive short-lived heap allocations during high-volume ingestion, triggering frequent JVM GC pauses.
* **Solution:** Acknowledge the `Group` API as a learning tool. For production throughput, plan a migration path to low-level `RecordConsumer` streams or vectorized column batching (e.g., Apache Arrow / Netty Off-Heap buffers).
