# Module 09: Vectorized Batch Processing

## Overview

Modules 04-07 implement a **row-oriented** engine: `ParquetTableReader` emits `Map<String,Object>` records, `TableScanOperator` groups them into `RecordBatch` objects, and `FilterOperator`/`AggregationOperator` work row by row. Every value is boxed, every row allocates a `Row`, and every operator makes a virtual call per row.

Module 09 adds a second execution world built on **Arrow columnar batches**: decoded values land in off-heap buffers, and operators read and write those buffers directly. Row and vector pipelines stay isolated; they share only the Parquet file format and the S3 configuration (see [Interop](#7-interop-with-the-row-world)).

The module covers:

1. a vectorized reader that materializes requested columns into Arrow vectors;
2. a vectorized writer that encodes Arrow batches into Parquet;
3. a vectorized scan operator and `VectorBatch` type, with selection-based filter and projection operators;
4. off-heap memory accounting and projection pushdown.

## 1. Two Execution Worlds

| Concern | Row world | Vector world |
|---|---|---|
| Batch type | `model.RecordBatch` (`List<Row>` of `Map`) | `vector.VectorBatch` (`VectorSchemaRoot`) |
| Memory | JVM heap objects | Arrow off-heap buffers |
| Source | `reader.RowReader` (`Map` per row) | `reader.VectorizedSource` (`VectorBatch`) |
| Scan | `operator.TableScanOperator` | `operator.VectorizedTableScanOperator` |
| Filter | `operator.FilterOperator` | `operator.VectorizedFilterOperator` |
| Project | folded into the scan (projected fields) | `operator.VectorizedProjectOperator` (computed/reordered only) |
| Driver | `query.QueryExecutor` + `query.PipelineExecutor` | not implemented; tests drive the stages |

## 2. The Vector Batch Model

`vector.VectorBatch` is a thin owner of an Arrow `VectorSchemaRoot`:

```java
public class VectorBatch implements AutoCloseable {
    private final VectorSchemaRoot root;
    public VectorSchemaRoot getRoot() { return root; }
    public int getRowCount() { return root.getRowCount(); }
    public Schema getSchema() { return root.getSchema(); }
    public void close() { root.close(); }
}
```

A `VectorSchemaRoot` holds one `FieldVector` per column. Each vector has:

* a **validity bitmap** (1 bit per row, 1 = non-null),
* a **data buffer** (fixed-width values, or offsets for variable-width),
* for `VarCharVector`, an **offset buffer** of `rowCount + 1` 32-bit offsets into the data buffer.

The batch schema travels with the data, so every operator below the source derives its shape from the upstream `VectorBatch` rather than holding its own copy.

```text
VectorBatch
  ├─ IntVector    "id"       validity + [int32 data]
  ├─ VarCharVector "country" validity + [offsets] + [utf8 bytes]
  └─ Float8Vector "revenue"  validity + [float64 data]
```

## 3. Pipeline Contracts

Two interfaces separate a source from a transform:

```java
public interface VectorizedSource extends AutoCloseable {
    VectorBatch nextBatch(BufferAllocator allocator) throws IOException; // null at EOF
    Schema getSchema();
    void close() throws IOException;
}

public interface VectorizedOperator extends AutoCloseable {
    void open();
    VectorBatch next(VectorBatch input, BufferAllocator allocator);
    void close();
}
```

`VectorizedTableScanOperator` adapts a `VectorizedParquetTableReader` to `VectorizedSource` and owns the batch size:

```text
VectorizedParquetTableReader ──► VectorizedTableScanOperator ──► VectorizedFilterOperator ──► VectorizedProjectOperator
        (decode)                  (scan stage)                 (selection + gather)          (computed projection)
```

The row-side analogue is `RowReader` → `TableScanOperator` → `FilterOperator`.

## 4. Decoding into Arrow Vectors

`VectorizedParquetTableReader` opens the file once, tracks row-group state (`currentRowGroup`, `rowIndexInGroup`, per-column readers), and hands out `VectorBatch`es of up to `batchSize` rows.

For each column present in the supplied Arrow schema it chooses a decoder from the column-chunk metadata:

| Column shape | Decoder | Strategy |
|---|---|---|
| Required flat INT32/INT64/FLOAT/DOUBLE, PLAIN or dictionary pages | `FixedWidthPageDecoder` | whole-page decode, bulk copy into Arrow buffers |
| Required flat BINARY/UTF-8, PLAIN or dictionary pages | `StringPageDecoder` | page parsed into offsets + data, then `setSafe` |
| Optional/repeated columns, `DELTA_*`, `BYTE_STREAM_SPLIT`, other types | `ColumnReaderDecoder` | per-value `ColumnReader` fallback |

The bulk decoders pull pages from `PageReader` themselves:

* **PLAIN fixed-width**: the page becomes one `ByteBuffer` view and values are copied into the Arrow data buffer with a single `setBytes` per batch slice; validity is filled with `0xFF` bytes.
* **Dictionary**: the dictionary page is decoded once; data-page indices are expanded in bulk by `ParquetRleDecoder` (one `Arrays.fill` per RLE run, `Packer.unpack32Values` for bit-packed runs), then mapped to values.
* **PLAIN strings**: each `[length][bytes]` page is parsed once into `int[] offsets` and `byte[] data`; values are written with `VarCharVector.setSafe`.

Supported Arrow vectors: `IntVector`, `BigIntVector`, `Float4Vector`, `Float8Vector`, `BitVector`, `VarCharVector`. Delta-encoded columns currently take the per-value fallback: correct, but not bulk-decoded.

### 4.1 Projection pushdown

Only columns present in the `arrowSchema` passed to the constructor are opened. There is no post-read pruning: passing `{id, revenue}` means the `country` column is never opened, paged, or decoded. Each row group logs the result:

```text
**** [VECTORIZED PARQUET READ] row group 0: decoded 2 of 3 file columns
     (projection pushdown), bulk=2 fallback=0, Arrow off-heap batches *****
```

The caller must include every column an operator needs — predicate, group-by, and aggregate inputs — in that schema.

### 4.2 Batch continuation

Row-group state survives across `nextBatch` calls: the in-group row offset continues inside the current row group, the next group is opened only when the current one is exhausted, and every vector is sized with `setInitialCapacity(rowsToRead)` before `allocateNew()`. A batch size smaller than the row group therefore does not lose rows.

### 4.3 Off-heap evidence in the log

On the first batch the reader logs the allocator's accounted off-heap bytes:

```text
**** [VECTORIZED PARQUET READ] first off-heap batch: rows=8,192
     allocated=131,072 bytes, peak=262,144 bytes *****
```

Subsequent batches log the same details at `FINE`.

## 5. Vectorized Operators

### 5.1 `VectorizedFilterOperator`

The filter builds a **selection vector** in one pass over the predicate column, then performs one **gather per column**:

```java
int[] selected = new int[rows];
int count = 0;
for (int i = 0; i < rows; i++) {
    if ((!hasNulls || !target.isNull(i)) && predicate.test(target, i)) selected[count++] = i;
}
```

Fixed-width gather reads values straight from the source data buffer and writes them into the destination buffer; validity is copied in bulk (`0xFF` fill) when the source has no nulls, otherwise one bit per copied row. Variable-width values are gathered by writing offsets and data directly (`gatherVarChar`), one memcpy per surviving value.

Predicates are typed factories so no boxing occurs:

```java
new VectorizedFilterOperator("revenue", VectorizedFilterOperator.reals(v -> v > 1000));
new VectorizedFilterOperator("country", VectorizedFilterOperator.strings("USA"::equals));
```

### 5.2 `VectorizedProjectOperator`

Projection after the scan cannot save I/O, so this operator exists for **computed or reordered** columns (for example `quantity * price AS revenue`), not for plain column pruning. Plain projections belong in the reader's Arrow schema.

## 6. Writing Vector Batches

`VectorizedParquetTableWriter` writes Arrow `VectorBatch`es to Parquet:

* **Bulk path** (`BulkParquetWriter`) for required flat INT32/INT64/FLOAT/DOUBLE/BINARY schemas: one row group per batch, one page per column, straight from Arrow buffers.
  * Encoding is chosen **per column**, either by the default policy or by a caller-supplied `Function<String, ColumnEncoding>`:
    * `DICTIONARY` → dictionary page + RLE/bit-packed indices (`RLE_DICTIONARY`);
    * `DELTA` → `DELTA_BINARY_PACKED` for INT32/INT64, `DELTA_LENGTH_BYTE_ARRAY` for BINARY;
    * `PLAIN` → raw values.
  * Default policy: INT64 (ids/timestamps) uses `DELTA`; BINARY/INT32/FLOAT/DOUBLE use `DICTIONARY`; a dictionary that exceeds the page threshold falls back to `PLAIN`, and `DELTA` on a type without a delta encoding falls back to dictionary/PLAIN.
  * Min/max statistics are computed while scanning the vectors, so files remain prunable; SNAPPY compression comes from `ColumnChunkPageWriteStore`.
  * The vector reader uses parquet-mr's `ColumnReader`, so it decodes every encoding the writer emits (PLAIN, dictionary, delta) without extra code.
* **Fallback path** (`WriteSupport<RowSlice>` + `RecordConsumer`) for any other schema. Parquet counts one record per `ParquetWriter.write(...)` call, so the fallback feeds one row per call; emitting several messages per call would make the footer row count disagree with the data.

Both paths produce standard Parquet, so files can be read by the row-based `ParquetTableReader` as well.

## 7. Memory and Lifecycle

* All vectors allocate from a `BufferAllocator` (typically `RootAllocator` with `arrow-memory-netty`), i.e. **off-heap direct memory**.
* Ownership is per batch: whoever produces a `VectorBatch` owns it; closing it releases the buffers.
* `allocator.getAllocatedMemory()` / `getPeakMemoryAllocation()` expose the accounted off-heap bytes. A healthy run returns to zero after all batches close; with `-Darrow.memory.debug.allocator=true`, leaks print an allocation stack.
* Batch sizing matters: `VectorSchemaRoot.create(...)` alone allocates Arrow's default capacity. The reader sets `setInitialCapacity(rowsToRead)` on every vector before `allocateNew()`.

## 8. Interop with the Row World

The in-memory formats are isolated, but the file format is a shared contract: both worlds read and write standard Parquet.

* `testRowWriterFileReadByVectorReader` — row-written file (dictionary encoded) read by `VectorizedParquetTableReader`.
* `testBulkWriterFileReadByRowReader` — vector-bulk-written file read by `ParquetTableReader` (Group API).
* The Arrow schema must match the file's field names and types; unsupported Arrow vector types throw at read time.

## 9. Verification

```bash
mvn test -Dtest=VectorizedParquetTableWriterReaderTest
```

Three end-to-end cases over MinIO:

* **vector write → vector read** — `VectorizedParquetTableWriter` then `VectorizedParquetTableReader` via `VectorizedTableScanOperator`;
* **vector write → row read** — vector-written file read by `ParquetTableReader` (Group API);
* **row write → vector read** — `ParquetTableWriter` output read into Arrow batches.

Each case checks all values (`id`, `country`, `revenue`) so encoding/decoding is exercised across both worlds; the reader logs show `bulk=3 fallback=0` and the off-heap batch allocation.

## 10. Current Limitations

* Bulk decoding covers PLAIN and dictionary pages for required flat columns; `DELTA_*`/`BYTE_STREAM_SPLIT` encodings and optional/repeated columns use the per-value `ColumnReader` fallback.
* Filtering uses a column-buffer selection loop plus typed gather; it is not SIMD-vectorized (no Java Vector API kernels).
* Writing supports required flat columns; optional/repeated or unsupported schemas take the row-at-a-time fallback.
* No vectorized aggregation (`GROUP BY` remains row-side).
* No vector query executor: no split planning, parallelism, or exchange above the scan. Tests drive the stages directly.
* Projection is caller-supplied; there is no planner computing `select ∪ predicate ∪ group keys ∪ aggregate inputs` for the vector path.
* Supported Arrow types are limited to Int32, Int64, Float32, Float64, Bit and Utf8.
