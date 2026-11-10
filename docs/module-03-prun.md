# Module 03: Control Plane Read Path, Metadata Pruning

## Overview
Module 03 implements the read-side control plane of our lakehouse connector. Instead of issuing expensive `GET` requests to S3 to inspect Parquet footers for every file in a table, the query planner evaluates partition prefixes and column-level statistics (`ColumnStats`) directly against the versioned table metadata (`vN.json`) in-memory.

## Architecture & Design

### 1. Zero S3 I/O During Planning
When a query executes, `TableScanPlanner` loads the latest table metadata snapshot from S3. The planning phase (`SplitPlanner`) executes entirely in-memory:
* **Partition Pruning:** Filters out data files whose storage paths do not match the requested partition prefix (e.g., `dt=2026-11-04`).
* **Statistics Bounds Pruning:** Evaluates query predicates (e.g., `amount > 300.0`) against the pre-computed `lower-bound` and `upper-bound` values stored in each file's `ColumnStats`. If a file's range cannot possibly satisfy the predicate, the file is skipped entirely.

### 2. Core Components
* **`ColumnStats`**: Stores min and max string/numeric bounds and null counts for data file columns.
* **`SplitPlanner`**: Core planning engine containing partition filtering and `isValidByStats` range evaluation logic.
* **`TableScanPlanner`**: Orchestrates loading table metadata from S3 and invoking the planner.
* **`TableSplit`**: Represents an executable file split containing path, record count, and schema version.

## 3. Verification & Testing
* **`SplitPlannerTest`**: Fast unit tests verifying partition pruning and column statistics range checks (`GREATER_THAN`, `LESS_THAN`, `EQUALS`) for both numeric and string values.
* **`TableReaderServiceTest`**: End-to-end integration test validating the full round-trip: committing transaction metadata via `TableWriterService` to MinIO/S3, and planning the scan via `TableScanPlanner`.