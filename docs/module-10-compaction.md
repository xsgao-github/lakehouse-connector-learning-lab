# Module 10: File Compaction & Small-File Maintenance

## Overview

Modules 04-07 build a row-oriented scan engine, and Module 09 adds a vectorized path on top of the same Parquet files. Both read whatever files the active snapshot lists. That model works until the table accumulates many tiny files: each append creates another Parquet object, and every object costs a metadata entry, a split, a footer read, and an S3 GET.

Module 10 adds a maintenance pass that rewrites small files into fewer larger files with the existing row reader/writer stack. The pass plans by partition, bin-packs inputs by target size, streams records through `ParquetTableReader` → `ParquetTableWriter`, recovers footer statistics for the new files, and publishes a new snapshot through the same OCC commit path used by normal writes. Old files are left in place for time travel; there is no vacuum.

The module covers:

1. `CompactionOptions`, the policy and thresholds;
2. `CompactionPlanner`, a pure planner that groups small files by partition and bins them;
3. `CompactionService`, the rewrite and commit orchestration;
4. metadata-overhead measurements before and after a compaction pass.

## 1. The Small-File Problem

A small file is not just "a file with few rows". It multiplies work across every layer:

| Cost | Per file |
|---|---|
| Metadata | One `DataFile` entry (path, record count, size, per-column stats, schema id) in a snapshot delta |
| Planning | One `TableSplit` and one pipeline task |
| Read setup | One `ParquetFileReader.readFooter` before any row is decoded |
| S3 | One metadata GET plus one data GET |

```text
before compaction                      after compaction
data/dt=2026-11-12/                    data/dt=2026-11-12/
  part-001.parquet  1 KB                 part-compacted-3f9a1c2e-000.parquet  5 KB
  part-002.parquet  1 KB               data/dt=2026-11-13/
  part-003.parquet  1 KB                 part-001.parquet                     1 KB
  part-004.parquet  1 KB
data/dt=2026-11-13/
  part-001.parquet  1 KB               5 metadata entries -> 2
  5 splits            -> 2             5 footers          -> 2
```

The catalog model matters here. Each `Snapshot` records a delta: `added-files` and `removed-files`. `TableMetadata.resolveLiveFiles()` replays those deltas from the first snapshot to the active one, keyed by file path, so a compaction commit only needs to add its outputs and remove its inputs. Legacy snapshots that listed the full live set still resolve because later entries overwrite earlier paths.

## 2. Compaction Architecture

| Component | Responsibility |
|---|---|
| `CompactionOptions` | Thresholds, target size, row-group size, codec |
| `CompactionPlanner` | Pure policy: partition grouping, small-file filter, bin packing |
| `CompactionService` | Load metadata, rewrite bins, recover stats/sizes, commit, measure |
| `ParquetTableReader` | Streams every record from an input file, resolving its `schema-id` and mapping columns by field id |
| `ParquetTableWriter` | Writes one output file per bin |
| `TableWriterService` + `OCCCommitHandler` | Publish the new snapshot with an atomic `If-None-Match: *` PUT |
| `CatalogService` | Load the active metadata and reload the committed version for metrics |

```text
CatalogService ──► CompactionPlanner ──► CompactionService.rewriteBin ──► TableWriterService
  load metadata       plan groups/bins       read -> write -> stats          OCC commit vN+1
```

The public surface:

```java
CompactionService service = new CompactionService(s3Client, bucketName);
CompactionService.Result result = service.compactTable("orders", CompactionOptions.defaults());
```

`Result` reports both the rewrite counters and the before/after metadata sizes. `committed == false` means the pass found nothing to do and wrote no metadata version.

## 3. Policy: What to Compact

`CompactionOptions` has production-leaning defaults and validates every value:

| Option | Default | Meaning |
|---|---|---|
| `smallFileThresholdBytes` | 8 MiB | A file is small when `file-size-in-bytes < threshold` |
| `minInputFilesPerGroup` | 3 | A partition needs at least this many small files |
| `targetFileSizeBytes` | 128 MiB | Bin capacity, estimated from declared input sizes |
| `rowGroupSizeBytes` | 8 MiB | Passed to `ParquetTableWriter` |
| `codec` | SNAPPY | Output compression |

The planner derives the partition from the file's parent directory (`data/dt=2026-11-12/part-001.parquet` → `data/dt=2026-11-12`), which matches the path-based pruning in `SplitPlanner`. A file at the table root belongs to the `""` partition.

Per partition:

* keep files whose declared size is strictly below the threshold;
* skip the partition entirely when its small-file count is below `minInputFilesPerGroup`;
* large files are never consumed and remain in the live set untouched.

Strictly-less-than matters: a file exactly at the threshold is already considered healthy. `minInputFilesPerGroup` must be at least 2 because rewriting a single file cannot reduce file count.

## 4. Bin Packing

Each qualifying partition is packed with **first-fit decreasing**:

1. sort small files by size descending, then path ascending;
2. place each file in the first bin where `used + size <= targetFileSizeBytes`;
3. otherwise open a new bin.

Each bin becomes exactly one output file. The algorithm is deterministic, so input ordering never changes the result.

```text
inputs: 60, 50, 40, 30, 20   target: 100

sorted: 60 50 40 30 20
bin 0:  60 + 40 = 100
bin 1:  50 + 30 + 20 = 100
```

A single input larger than the target gets its own oversized bin. With the defaults (8 MiB threshold, 128 MiB target) that cannot happen, but the planner does not reject it.

Bin capacity uses the **declared input size**. Parquet re-encodes and compresses on rewrite, so an output can be smaller or larger than its inputs; the target is a heuristic, not a hard limit.

## 5. Rewriting a Bin

`CompactionService.rewriteBin` opens one writer per bin and streams every input through the full catalog schema:

```java
try (ParquetTableWriter writer = new ParquetTableWriter(
        schema, outputPath, options.getRowGroupSizeBytes(), options.getCodec())) {
    for (DataFile input : bin) {
        try (ParquetTableReader reader = new ParquetTableReader(schema, null, inputPath)) {
            Map<String, Object> record;
            while ((record = reader.readNextRecord()) != null) {
                writer.writeRecord(record);
                records++;
            }
        }
    }
}
```

Compaction must not project: `projectedFields = null` reads every column, because dropping a column would silently change the table.

After the writer closes and the footer is uploaded:

* `extractFooterStatistics(outputPath)` rebuilds the per-column min/max/null stats, so the compacted file stays prunable by the Module 3 planner;
* `s3Client.headObject(...).contentLength()` supplies the real `file-size-in-bytes` (wrapped in `S3RetryExecutor` from Module 8);
* the output becomes a `DataFile(relativePath, records, size, columnStats, currentSchemaId)`.

`writeRecord` emits exactly one Parquet record per call, so the counted `records` equals the footer row count.

## 6. Output Naming and Collision Avoidance

Outputs stay in their input partition directory:

```text
data/dt=2026-11-12/part-compacted-3f9a1c2e-000.parquet
data/dt=2026-11-12/part-compacted-3f9a1c2e-001.parquet
```

`3f9a1c2e` is a per-run UUID prefix; `000`, `001`, … is a global output index across all groups and bins. `ExampleParquetWriter` uses CREATE mode, so an existing object makes the write fail rather than overwrite. A fresh run id makes collisions practically impossible, and a failed run that is retried gets a new id.

## 7. Publishing the New Snapshot

The commit publishes a delta, not a replacement snapshot:

```text
v1 snapshot: added [A1 A2 A3 A4 B1], removed []
v2 snapshot: added [C1],             removed [A1 A2 A3 A4]   (C1 = compacted A1-A4)

resolved live set after v2: [B1 C1]
```

`CompactionService` calls:

```java
commitTableTransaction(tableName, schema, compactedFiles, removedFiles, currentSnapshotId)
```

The new snapshot records only `compactedFiles` in `added-files` and the consumed inputs in `removed-files`. `TableMetadata.resolveLiveFiles()` replays the chain and returns `[B1 C1]`; `SplitPlanner` plans from that resolved set. The OCC handler publishes `metadata/v<N+1>.json` with `If-None-Match: *` and retries on a version collision.

Old files remain physically present, and older metadata versions still reference them. Time travel and readers that started before the commit keep working. Reclaiming that storage is a separate vacuum concern and is not implemented here.

When no group qualifies, `compactTable` returns `committed = false` and does not call the commit path, so an idle maintenance run creates no empty snapshot versions.

## 8. Measuring Metadata Overhead

The integration test compacts four old-schema files plus one new-schema file in one partition into one. Observed metrics:

| Metric | Before | After |
|---|---|---|
| Resolved live files | 5 | 1 |
| Active-snapshot JSON | 2,721 bytes | 3,358 bytes |
| Full `metadata/vN.json` | 5,728 bytes | 9,087 bytes |
| Splits for a full scan | 5 | 1 |
| Physical objects | 5 | 6 |

The resolved live set shrinks from five files to one, which is what planning and reading pay for. The active snapshot JSON **grows** because it now records the delta (`added-files` plus `removed-files`) and carries a `schema-id`; the full metadata also grows because it retains snapshot and schema history. That is expected: the saving is in the resolved file set and future planning, not in the historical record.

```text
**** [COMPACTION METRICS] entries 5 -> 1, active snapshot JSON 2,721 -> 3,358 bytes,
     metadata JSON 5,728 -> 9,087 bytes *****
```

`Result` exposes both pairs so callers can tell the two effects apart.

## 9. Failure, Cleanup, and Concurrency

* **Rewrite failure before commit**: `CompactionService` tracks every output key it created and best-effort deletes them. Partial Parquet objects are unreferenced by any snapshot, so they are harmless, but deleting them keeps the bucket tidy. The original exception is rethrown and no metadata version is written.
* **Failure after commit starts**: outputs are left in place. The atomic PUT may have succeeded while the client saw a timeout, and deleting outputs could corrupt a committed snapshot. The log marks them as possibly orphaned.
* **Concurrent writers**: metadata is loaded at the start of the pass and the commit happens later. OCC only detects a `vN.json` collision, not a changed live set, so a writer that commits between the load and the compaction PUT can be lost. Run compaction as a single-writer maintenance job. A lease or a compare-and-swap on `current-snapshot-id` would be required to solve this properly and is out of scope.
* **No vacuum**: old small files and historical snapshots stay in S3 by design.

## 10. Verification

```bash
mvn test -Dtest=CompactionServiceTest
mvn test
```

`CompactionServiceTest.compactsSmallFilesAfterSchemaEvolutionIntoOneFileReadersUse` writes four files with the v1 schema and one with an evolved v2 schema (field id 2 renamed to `full_name`, nullable `region` added) over MinIO, commits both metadata versions, compacts all five, and checks that:

* the resolved live set changes from five files to one;
* the active snapshot is a delta: one `added-files` entry and five `removed-files` entries, both stamped with `schema-id` 2;
* the active snapshot, `SplitPlanner`, and `QueryExecutor` all see exactly one compacted file;
* all ten records are readable: the eight old rows are mapped by field id to `full_name`, return `region = null`, and no longer expose `name`; the two new rows keep `US`/`EU`;
* the compacted file carries `schema-id` 2 and `full_name`/`region` column statistics;
* the full metadata JSON retains snapshot and schema history;
* the original files remain in S3 (no vacuum).

To inspect the objects manually, run `mvn test -Dtest=CompactionServiceTest -DkeepData=true` and open MinIO at `localhost:9001`.

## 11. Current Limitations

* No vacuum or garbage collection; old files and historical snapshots remain forever.
* Compaction is single-threaded and processes one bin at a time.
* Bin sizing uses declared input sizes; compression drift means output files can exceed the target.
* No incremental or resumable compaction; every pass re-plans the whole active snapshot.
* No cross-partition consolidation: files are only merged inside their own partition directory.
* Only the row reader/writer stack is used; a vectorized rewrite could reuse Module 09 but is not implemented.
* Concurrent writers can invalidate the live-set payload (lost update); there is no lock or CAS on the current snapshot.
* Compaction reduces file count and live-manifest size, but does not guarantee fewer physical bytes.
* The row path resolves each file's `schema-id` and maps columns by field id, so additions, renames, drops, and simple type coercion are readable. The vectorized path is still name-based and only supports additive nullable columns.
* Snapshots store deltas inline in `vN.json`; there are no manifest files, so replay cost grows with the number of snapshots.
* Legacy metadata without `schemas` or `schema-id` falls back to the single current schema and name matching.
