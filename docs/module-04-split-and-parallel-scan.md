# Module 04: File Splitting and Parallel Operator Pipelines

Module 04 connects metadata pruning to real data-plane execution. Each planned `TableSplit` receives an independent operator pipeline, a bounded worker pool drains those pipelines concurrently, and the executor returns their batches in input split order.

## 1. Control Plane vs. Data Plane

* **Control Plane (`TableReaderService` + `SplitPlanner`)**
  * Loads versioned table metadata through `CatalogService`.
  * Resolves the active snapshot and applies partition and column-statistics pruning.
  * Produces `TableSplit` tasks without reading data files.
* **Data Plane (`ScanExecutor`)**
  * Constructs one operator pipeline per split through `Function<TableSplit, Iterator<RecordBatch>>`.
  * Executes pipelines with a bounded `ExecutorService` thread pool.
  * Flattens per-split batches in input split order.

## 2. Core Components

### A. Pipeline Factory

Uses Java's standard `Function` to create a fresh operator chain for each split:

```java
Function<TableSplit, Iterator<RecordBatch>> pipelineFactory = split -> {
    RowReader reader = createReaderFor(split);
    return new TableScanOperator(reader, projectedFields);
};
```

The factory runs **inside the worker task** because reader and operator constructors may perform I/O or prefetch. Readers and operators contain mutable iterator state, so every split owns its complete chain.

A Parquet pipeline typically contains:

```text
ParquetTableReader → TableScanOperator → optional FilterOperator → optional AggregationOperator
```

### B. `ScanExecutor`

```java
public List<RecordBatch> execute(
    List<TableSplit> splits,
    Function<TableSplit, Iterator<RecordBatch>> pipelineFactory) throws Exception {}
```

The executor:

1. Uses a pool of `min(splitCount, maxThreads)` workers.
2. Creates and drains one pipeline per split.
3. Reads futures in submission order, even if workers finish out of order.
4. Returns one flattened `List<RecordBatch>`.

### C. Ordinary Scan, Projection, and Filtering

No separate combiner interface is needed. The executor directly returns the flattened batches:

```java
List<RecordBatch> results = readerService.executeScan(
    tableName,
    partitionFilter,
    predicates,
    split -> {
        RowReader reader = createReaderFor(split);
        Iterator<RecordBatch> scan = new TableScanOperator(reader, projectedFields);
        return new FilterOperator(scan, predicate);
    }
);
```

### D. Aggregation: Partial Results and Conceptual Final Combine

For aggregation, each split pipeline ends with a local `AggregationOperator`:

```java
List<RecordBatch> partialBatches = readerService.executeScan(
    tableName, partitionFilter, predicates, split -> {
        Iterator<RecordBatch> scan = createScanAndFilterFor(split);
        return new AggregationOperator(scan, groupByKeys, aggregations);
    }
);
```

The current `AggregationOperator` performs only local aggregation. Final cross-split combining is intentionally conceptual in this learning lab and has no dedicated implementation class.

Final aggregation rules:

| Local aggregate | Final reduction |
|---|---|
| `SUM` | Sum partial sums |
| `COUNT` | Sum partial counts |
| `MIN` | Minimum of partial minimums |
| `MAX` | Maximum of partial maximums |

Final expressions read partial output aliases. In particular, final `COUNT` must not count partial rows.
