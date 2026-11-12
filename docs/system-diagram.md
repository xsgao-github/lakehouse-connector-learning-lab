# System Diagrams

The diagrams below reflect the **current implementation**:

- `QueryExecutor.execute()` loads metadata, plans splits, runs independent operator pipelines concurrently through `PipelineExecutor`, and returns flattened `RecordBatch` results in input split order.
- `AggregationOperator` implements local, per-split aggregation; a cross-split final combine remains conceptual.
- `PipelineExecutor` (package-private) drains each split's pre-assembled operator pipeline inside a worker task.

## 1. Metadata loading, pruning, and split execution

```mermaid
sequenceDiagram
    autonumber
    actor Client
    participant QE as QueryExecutor
    participant Planner as TableScanPlanner
    participant Catalog as CatalogService
    participant Split as SplitPlanner
    participant Executor as PipelineExecutor
    participant W1 as Worker 1
    participant WN as Worker N
    participant S3 as S3 / MinIO

    Client->>QE: execute(table, filter, predicates, readerFactory)

    Note over QE,S3: Control plane: discover and load table metadata
    QE->>Planner: loadTableMetadata(table)
    Planner->>Catalog: loadTableMetadata(table)
    Catalog->>Catalog: discoverLatestVersion(table)
    Catalog->>S3: LIST tables/{table}/metadata/
    S3-->>Catalog: metadata object keys
    Catalog->>Catalog: select highest vN.json
    Catalog->>S3: GET tables/{table}/metadata/vN.json
    S3-->>Catalog: metadata JSON
    Catalog->>Catalog: TableMetadata.fromJson(...)
    Catalog-->>Planner: TableMetadata
    Planner-->>QE: TableMetadata

    Note over QE,Split: Planning uses metadata only, so no Parquet data is read
    QE->>Planner: planSplits(metadata, partitionFilter, predicates)
    Planner->>Split: planSplits(metadata, partitionFilter, predicates)
    Split->>Split: find Snapshot where snapshotId = currentSnapshotId

    loop Every DataFile in activeSnapshot.addedFiles
        alt partitionFilter is present and filePath does not contain it
            Split->>Split: partition-prune file
        else column min/max statistics exclude a predicate
            Split->>Split: statistics-prune file
        else file survives pruning
            Split->>Split: create TableSplit<br/>(filePath, recordCount, fileSize, schemaId)
        end
    end

    Split-->>Planner: List of surviving TableSplits
    Planner-->>QE: List of surviving TableSplits

    Note over QE,Executor: Data plane: execute one independent operator pipeline per split
    QE->>Executor: execute(splits, split -> new TableScanOperator(readerFactory.apply(split)))
    Executor->>Executor: create fixed thread pool<br/>size = min(splitCount, maxThreads)

    par Split 1
        Executor->>W1: pipelineFactory.apply(split1)
        W1->>W1: construct RowReader and TableScanOperator
        W1->>S3: read projected Parquet data for split1
        S3-->>W1: column data
        W1->>W1: drain pipeline into RecordBatches
        W1-->>Executor: split1 result batches
    and Split N
        Executor->>WN: pipelineFactory.apply(splitN)
        WN->>WN: construct RowReader and TableScanOperator
        WN->>S3: read projected Parquet data for splitN
        S3-->>WN: column data
        WN->>WN: drain pipeline into RecordBatches
        WN-->>Executor: splitN result batches
    end

    Executor->>Executor: Future.get() in input split order<br/>flatten per-split batches
    Executor-->>QE: ordered List of RecordBatches

    QE-->>Client: List of RecordBatches
```

**Pruning order:** active snapshot → partition-path filter → column min/max statistics → `TableSplit`.

For time travel, `planTimeTravelScan()` loads the requested `vX.json` directly, then performs the same planning and pruning. It returns splits without executing them.

Each split constructs its own reader and `TableScanOperator` inside a worker task. `QueryExecutor.execute` performs no filtering or aggregation: it only materializes scan batches, and `PipelineExecutor` flattens them directly.

## 2. Real Parquet reads through the operator pipeline

```mermaid
sequenceDiagram
    autonumber
    actor Client
    participant Planner as TableScanPlanner
    participant Exec as PipelineExecutor
    participant Ops as FilterOperator /<br/>AggregationOperator
    participant Scan as TableScanOperator
    participant Parquet as ParquetTableReader<br/>(implements RowReader)
    participant S3 as S3 / MinIO

    Client->>Planner: planScan(table, partitionFilter, predicates)
    Planner-->>Client: pruned TableSplits

    Note over Client,Parquet: The caller constructs the format-specific RowReader
    Client->>Client: resolve projectedFields
    Client->>Parquet: new ParquetTableReader(<br/>schema, projectedFields, path, ...)
    Parquet->>Parquet: derive projected column names from projectedFields
    Parquet->>S3: read Parquet footer through Hadoop S3A
    S3-->>Parquet: footer and physical file schema
    Parquet->>Parquet: derive requested schema<br/>containing projected columns only
    Parquet->>Parquet: configure ParquetReader projection
    Client->>Scan: new TableScanOperator(rowReader)

    opt Row filtering and/or aggregation
        Client->>Ops: wrap scan with FilterOperator<br/>and/or AggregationOperator
    end

    Client->>Exec: execute(splits, pipelineFactory)

    loop Until the pipeline is exhausted
        Exec->>Ops: hasNext() / next()
        Ops->>Scan: pull upstream RecordBatch

        Scan->>Scan: create RecordBatch from reader.getFields()

        loop Up to 1,000 rows or EOF
            Scan->>Parquet: readNextRecord()
            Parquet->>S3: read projected Parquet columns
            S3-->>Parquet: encoded column data
            Parquet->>Parquet: decode Group into row map
            Parquet-->>Scan: Map of column values
            Scan->>Scan: add Row to RecordBatch
        end

        Scan-->>Ops: RecordBatch

        opt FilterOperator
            Ops->>Ops: predicate.evaluate(row)<br/>discard non-matching rows
        end

        opt AggregationOperator
            Ops->>Ops: consume upstream batches<br/>group rows and calculate<br/>SUM / COUNT / MIN / MAX
        end

        Ops-->>Exec: result RecordBatch
        Exec->>Exec: append batch to results
    end

    Scan->>Parquet: close RowReader at EOF
    Exec-->>Client: List of RecordBatches
```

The pull-based operator call flow is:

```text
PipelineExecutor.execute() (per split)
    → FilterOperator / AggregationOperator
    → TableScanOperator
    → ParquetTableReader (RowReader)
    → S3 / MinIO
```

## 3. Parquet writing and optimistic metadata commit

```mermaid
sequenceDiagram
    autonumber
    actor Client
    participant Writer as ParquetTableWriter
    participant Footer as ParquetTableReader
    participant S3 as S3 / MinIO
    participant OCC as OCCCommitHandler
    participant Service as TableWriterService
    participant Catalog as CatalogService

    Note over Client,S3: Physical data is written before metadata is committed
    Client->>Writer: new ParquetTableWriter(schema, outputPath, ...)
    Writer->>Writer: convert table Schema to Parquet MessageType

    loop Every input record
        Client->>Writer: writeRecord(row)
        Writer->>Writer: map schema fields to Parquet Group
        Writer->>S3: write Parquet data through Hadoop S3A
    end

    Client->>Writer: close()
    Writer->>S3: finish file and write Parquet footer

    opt Explicit WAP staging flow
        Client->>OCC: promoteStagingFiles(table, txnId, paths)
        loop Every staged file
            OCC->>S3: COPY _staging/txn_ID/...<br/>to data/txn_ID/...
            OCC->>S3: DELETE staged source
        end
    end

    opt Collect file statistics for future pruning
        Client->>Footer: extractFooterStatistics(path)
        Footer->>S3: read Parquet footer
        S3-->>Footer: column chunk statistics
        Footer-->>Client: min / max / null-count per column
    end

    Client->>Client: build DataFile(<br/>path, recordCount, fileSize, columnStats)

    Note over Client,Catalog: Commit publishes a new snapshot through versioned metadata
    Client->>Service: commitTableTransaction(<br/>table, schema, newFiles, parentSnapshotId)
    Service->>Service: collect newFiles.filePath<br/>as modifiedPartitions
    Service->>Catalog: isTableExists(table)
    Catalog->>S3: LIST metadata versions
    S3-->>Catalog: existing metadata keys

    alt Table exists
        Service->>Catalog: loadTableMetadata(table)
        Catalog->>S3: GET latest metadata/vN.json
        S3-->>Catalog: existing TableMetadata JSON
        Catalog-->>Service: existingMetadata
    else New table
        Catalog-->>Service: no existing metadata
    end

    Service->>Service: create VersionPayloadGenerator lambda
    Service->>OCC: commitWithOCC(<br/>table, modifiedPartitions, payloadSupplier)

    loop Retry until success or maxRetries
        OCC->>Catalog: discoverLatestVersion(table)
        Catalog->>S3: LIST tables/{table}/metadata/
        S3-->>Catalog: current metadata versions
        Catalog-->>OCC: currentVersion
        OCC->>OCC: targetVersion = currentVersion + 1

        OCC->>Service: payloadSupplier.generatePayload(targetVersion)
        Service->>Service: create new Snapshot(newFiles)
        Service->>Service: append snapshot history<br/>set currentSnapshotId
        Service->>Service: serialize new TableMetadata
        Service-->>OCC: commitJson

        OCC->>S3: PUT metadata/vTarget.json<br/>If-None-Match: *

        alt Metadata object did not exist
            S3-->>OCC: PUT succeeds
            OCC-->>Service: committed targetVersion
            Service-->>Client: committed version
        else Another writer created vTarget.json first
            S3-->>OCC: 412 PreconditionFailed
            OCC->>OCC: conflict detected
            OCC->>OCC: sleep(backoffMillis × attempt)<br/>then retry with a new targetVersion
        end
    end
```

### Key responsibilities

| Object | Responsibility |
|---|---|
| `CatalogService` | Discovers, reads, and writes versioned `vN.json` metadata. |
| `TableScanPlanner` | Orchestrates metadata loading and split planning; it never reads data files. |
| `QueryExecutor` | Caller entry point: loads metadata once, plans splits, and delegates per-split execution to `PipelineExecutor`. |
| `SplitPlanner` | Resolves the active snapshot and converts surviving `DataFile` entries into `TableSplit` objects. |
| `PipelineExecutor` | Package-private behind `QueryExecutor`; constructs and drains independent per-split pipelines concurrently, flattening batches in input split order. |
| `AggregationOperator` | Implements `Iterator<RecordBatch>`; computes per-split partial aggregates. Cross-split final reduction is conceptual. |
| `RowReader` | Format-independent contract for reading one decoded record at a time. |
| `TableScanOperator` | Converts rows from an injected `RowReader` into `RecordBatch` objects. |
| `ParquetTableWriter` | Physically writes Parquet records and footers. |
| `TableWriterService` | Builds new snapshot metadata and delegates the OCC commit. It does **not** write Parquet data. |
| `OCCCommitHandler` | Atomically creates the next metadata version using `If-None-Match: *`; retries on version conflicts. |

The overall write/read relationship is:

```text
Write:
rows → Parquet file → DataFile + statistics → Snapshot → atomic vN.json commit

Read:
vN.json → active Snapshot → DataFiles → pruning → TableSplits
       → independent ParquetTableReader (RowReader) + operator pipelines
       → concatenation OR final aggregate combining
       → RecordBatches
```

Relevant implementations: `QueryExecutor.java:46`, `PipelineExecutor.java:41`, `TableScanPlanner.java:33`, `SplitPlanner.java:24`, `RowReader.java:12`, `TableScanOperator.java:10`, `AggregationOperator.java:22`, `TableWriterService.java:29`, and `OCCCommitHandler.java:33`.
