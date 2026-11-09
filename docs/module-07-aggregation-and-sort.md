# Module 07: Edge Aggregators & Sorting Strategy

## Overview

Module 07 introduces the **Edge Aggregator** (`AggregationOperator`), a pipeline-breaking combiner operator situated downstream of the scan and filter layers. Instead of streaming raw, filtered records across the network to a coordinator node, edge workers perform **local partial aggregation** grouped by query keys (`GROUP BY`), drastically reducing data transfer volume.

## Core Architecture: `AggregationOperator`

### 1. Pipeline-Breaking Combiner Pattern

Unlike row-by-row or filtering operators (`FilterOperator`, `TableScanOperator`) which stream data lazily batch-by-batch, `AggregationOperator` is a **pipeline-breaking operator**. During its initial execution phase, it fully drains the upstream `Iterator<RecordBatch>`, constructs a local grouping map (`Map<List<Object>, double[]>`), and materializes the reduced results into an output `RecordBatch`.

### 2. Supported Accumulator Functions

`AggregationOperator` supports standard analytical reduction expressions (`AggFunction`):
* **`SUM`**: Accumulates running numeric totals.
* **`COUNT`**: Tracks record occurrences.
* **`MIN` / `MAX`**: Computes running extrema, initializing tracking states to `Double.POSITIVE_INFINITY` and `Double.NEGATIVE_INFINITY` respectively.

### 3. Implementation Summary

The operator inspects upstream batches lazily on the first invocation of `hasNext()`, builds output schema fields dynamically from grouping keys and aggregation aliases, and emits the reduced result batch.

## Cross-Split Final Combining

`ScanExecutor` gives each split an independent local `AggregationOperator`. A final cross-split combiner is a conceptual follow-on, not a current implementation. It would reduce the ordered partial batches as follows:

* `SUM`, `MIN`, and `MAX` reduce the corresponding partial values.
* `COUNT` adds numeric partial counts instead of counting partial rows.
* Final expressions read from the partial output aliases because original input columns are no longer present.
* `LinkedHashMap` preserves first-group encounter order, and final combining runs on the coordinator thread without shared accumulator state.

## Architectural Note: Local Sorting & Top-$K$ Strategy

While local sorting remains documented as a potential partition-aware optimization, it is explicitly omitted from the active execution path for the following reasons:

1. **Partition Awareness:** Parquet files in the lakehouse are already structured by partition and directory layout (e.g., `country=USA`). General cross-partition sorting on worker nodes introduces unnecessary CPU and memory pressure.
2. **Spill Overhead:** Unconstrained in-memory sorting risks spilling to disk or triggering `OutOfMemoryError` on high-cardinality datasets without delivering analytical value that partition pruning and projection pushdown haven't already achieved.

### Future Extension Guidelines
* **Bounded Top-$K$ Heaps:** If query semantics demand an explicit `ORDER BY col DESC LIMIT N`, implement a fixed-size priority queue rather than a full partition sort.
* **Streaming Merges:** Rely on pre-sorted file metadata properties (`LocalSortedProperty`) rather than active worker-side sorting.