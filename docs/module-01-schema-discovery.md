# Module 01: Schema Discovery & Versioned Metadata Management

## 1. Module Overview

Module 01 establishes the core storage-centric metadata management tier for the Lakehouse Connector. It provides an immutable, versioned catalog engine capable of tracking table schemas, snapshot histories, and evolution states (`v1.json` $\rightarrow$ `v2.json`) directly over MinIO/S3 without relying on external relational metadata databases or mutating central state files.

## 2. Architectural Design & Core Principles

### 2.1 Storage Layout & Naming Conventions

- **Bucket Layout:** All table metadata and snapshots are stored under predictable S3 prefixes within the configured warehouse bucket:

  ```
  s3://warehouse/tables/<table-name-kebab-case>/metadata/v<N>.json
  ```

- **Object Mapping Conventions:**

  * **JSON Schemas:** Standardized to **kebab-case** (e.g., `format-version`, `table-uuid`, `current-snapshot-id`, `parent-snapshot-id`, `added-files`).

  * **Java POJOs:** Standardized to **camelCase** (e.g., `formatVersion`, `tableUuid`, `currentSnapshotId`), mapped via Jackson `@JsonProperty` annotations.

  * **Data Types:** Field data types inside schemas are normalized to lowercase strings (e.g., `"long"`, `"double"`, `"string"`) during deserialization to guarantee case-insensitive comparisons.

### 2.2 Lab Implementation vs. Production Ecosystem

* **The Educational Lab Approach:** In this learning lab, we hand-code the metadata serialization, version incrementation, and structural schema evolution logic. This hands-on construction builds a deep, visceral understanding of how metadata files, immutable snapshots, and field-level lineage interact under the hood.
* **The Production Ecosystem Reality:** In enterprise environments, **engineers do not hand-code schema evolution or manifest maintenance.** Instead, production systems rely on battle-tested third-party libraries and query engine kernels (such as **PyIceberg**, **delta-rs**, **Delta Kernel**, or native execution layers in **Trino, Spark, and DuckDB**). These vendor libraries automatically handle complex schema evolution registries (maintaining multi-schema histories with distinct `schema-id` arrays), manifest compilation, file compaction planning, and OCC retry loops out of the box.

---

## 3. Class Definitions & Responsibilities

### 3.1 Data Models (`xsgao.lakehouseconnector.catalog`)

- **`TableMetadata`**: Root object representing complete table state at a specific version point. Holds `formatVersion`, `tableUuid`, `location`, `schema`, `currentSnapshotId`, and historical `snapshots`.

- **`Schema`**: Holds structural definition (`schemaId`, `fields`). Each metadata version records its schema; evolution is represented by incrementing `schemaId`.

- **`Field`**: Defines individual column metadata (`id`, `name`, `type`, `nullable`).

- **`Snapshot`**: Captures a point-in-time state of table data (`snapshotId`, `timestampMs`, `parentSnapshotId`, `addedFiles`).

- **`DataFile`**: Represents physical Parquet file entries (`filePath`, `recordCount`, `fileSizeBytes`).

### 3.2 Service Tier: `CatalogService`

The `CatalogService` acts as the exclusive manager of versioned metadata:

- **`isTableExists(tableName)`**: Checks S3 object listings to determine if table metadata exists.

- **`saveTableMetadata(tableName, metadata)`**:

  1. Scans existing objects under `tables/<table-name>/metadata/v*.json`.

  2. Auto-increments to the next available version integer $N + 1$.

  3. Writes `vN.json` to S3 and returns the newly committed version integer ($N + 1$).

- **`loadTableMetadata(tableName)`**: Discovers the highest version integer $N$ in S3 and loads `vN.json` (Latest Version pattern).

- **`loadTableMetadata(tableName, version)`**: Fetches a specific historical version `vN.json` (Time-Travel / Auditing pattern).

---

## 4. Common Pitfalls & Solutions

### 4.1 Column Renaming vs. Field ID Stability
- **Pitfall:** Relying solely on column names (e.g., `customer_email`) to track identity across schema versions. If a column is renamed, downstream query engines may treat it as a dropped column and a newly added column, breaking historical data lineage.
- **Solution (production):** **Immutable Field IDs.** Every field carries a permanent, unique integer ID (e.g., Field ID `3`). Schema evolution tracks fields by their integer IDs rather than names, so a rename updates the name associated with ID `3` while historical data files remain readable.
- **Implementation:** `TableMetadata` keeps a `schemas[]` history and a `current-schema-id`; every `Snapshot` and `DataFile` carries the `schema-id` it was written with. The row reader resolves that schema per file and maps columns by field id, so additions, renames, data-preserving drops, and simple type promotions are readable. The vectorized reader still maps by name and only supports additive nullable columns. Required-column additions with defaults and full type migration remain out of scope.

### 4.2 S3 "Last-Write-Wins" and Concurrency Collisions
- **Pitfall:** Two concurrent writer jobs attempting to commit `v2.json` simultaneously. Standard S3 `PutObject` operates on a last-write-wins model, causing one write to silently overwrite and corrupt another writer's commit.
- **Solution:** **Optimistic Concurrency Control (OCC) / Conditional Puts.** In single-writer engines, version discovery must resolve sequentially. For multi-writer setups, utilize S3 conditional headers (`If-None-Match: *`) to ensure writing `vN.json` fails if the key already exists, forcing the caller to reload and retry.

### 4.3 Jackson Deserialization Fragility
- **Pitfall:** As metadata specs evolve over time (e.g., adding partition specs or sort orders), older readers encounter unknown JSON properties and throw `UnrecognizedPropertyException`.
- **Solution:** Configure `ObjectMapper` with `FAIL_ON_UNKNOWN_PROPERTIES = false` to guarantee forward and backward compatibility when parsing metadata JSON documents.

### 4.4 Case-Sensitivity & Type Mismatches
- **Pitfall:** Storing inconsistent data type casing (e.g., `"LONG"` vs `"long"`) causes cross-engine compatibility bugs between engines like Spark, Trino, and custom connectors.
- **Solution:** **Boundary Normalization.** Enforce lowercasing of string types in `Field` constructors and setters before serialization.

### 4.5 Dangling Metadata Pointers & Premature Cleanup
- **Pitfall:** Deleting old metadata versions (`v1.json`) breaks time-travel queries and history auditing.
- **Solution:** **Append-Only Metadata Storage.** Treat metadata version files as strictly immutable and append-only. Never delete historical `vN.json` files during schema evolution.

---

## 5. Verification & Testing Strategy

### 5.1 Unit Testing: `TableMetadataSerdeTest`

Validates pure Jackson JSON serialization and deserialization locally in memory:

- Verifies exact kebab-case output matching.

- Ensures field type normalization (e.g., `"LONG"` $\rightarrow$ `"long"`).

- Verifies correct handling of `null` parent snapshot IDs for initial table commits.

### 5.2 Integration Testing: `CatalogServiceTest`

Validates full round-trip integration against MinIO using `S3ClientFactory`:

- **Isolated Execution:** Tests use dynamic unique table names (`test_orders_<uuid-suffix>`) to guarantee test independence and parallel safety.

- **Evolution Round-Trip:** Simulates writing initial `v1.json` state, evolving the metadata schema (adding a nullable column, incrementing `schemaId` to 2), and writing `v2.json` with a linked `parentSnapshotId`. This covers metadata evolution only; physical data files are not rewritten by the catalog.

- **Version Verification:** Verifies both automatic latest-version loading (`v2`) and historical snapshot retrieval (`v1`).

- **Bucket Hygiene:** Includes `@AfterEach` teardown logic to purge test-generated prefixes from MinIO after each run.