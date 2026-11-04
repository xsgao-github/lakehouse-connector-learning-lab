# Module 02: Optimistic Concurrency Control (OCC) & Write-Audit-Publish (WAP)

## 1. Overview

In distributed lakehouse architectures, multiple concurrent writers may attempt to modify the same table simultaneously. Without proper coordination, concurrent writes result in **lost updates**, where one writer silently overwrites another's data. 

This module implements **Optimistic Concurrency Control (OCC)** combined with a **Write-Audit-Publish (WAP)** pattern over object storage (S3/MinIO) to ensure atomicity, isolation, and data integrity without requiring heavy, synchronous distributed locking coordinators.

---

## 2. Architecture & Data Layout

The connector organizes table data and metadata using a strict directory convention:

```text
tables/
└── <table_name>/
    ├── metadata/
    │   ├── v1.json
    │   ├── v2.json
    │   └── v3.json
    ├── _staging/
    │   └── txn_<UUID>/
    │       └── data.csv
    └── data/
        └── txn_<UUID>/
            └── data.csv
```

* **`metadata/vN.json`**: Immutable versioned metadata manifests pointing to active snapshots and data files.
* **`_staging/txn_<UUID>/`**: Isolated transaction workspace where writers write uncommitted data files.
* **`data/txn_<UUID>/`**: Published data directory where files reside after a successful transaction commit.

## 3. How OCC Works: Checking and Resolving Concurrency

To guarantee that two writers cannot commit the same version (e.g., both trying to write `v2.json`), the connector uses atomic conditional operations. There are two primary architectural approaches to checking and enforcing OCC:

### 3.1 Approach 1: Object Store Conditional Puts (CAS) - *Native Approach*
Instead of locking tables via an external database, the connector leverages object storage **Compare-And-Swap (CAS)** semantics via HTTP precondition headers:
1. **Version Discovery**: The writer lists the `metadata/` prefix to find the highest existing version (e.g., `v1.json` $\rightarrow$ current version is `1`).
2. **Target Version Calculation**: Determines the next version (`targetVersion = currentVersion + 1`, e.g., `v2.json`).
3. **Atomic Conditional Put**: Issues a `PutObject` request for `v2.json` with the header:
   ```http
   If-None-Match: "*"
   ```
4. **Result Evaluation**:
   * If S3/MinIO returns `200 OK`, the commit succeeded.
   * If S3/MinIO returns `412 Precondition Failed` (or `PreconditionFailed` error code), another writer raced ahead and committed `v2.json` first. The handler catches this, backs off with jitter, re-discovers the latest version, and retries.

### 3.2 Approach 2: External Metastore / Database Locking - *Alternative Approach*
In systems where object storage conditional headers are either not desired or require centralized tracking:
* **Catalog Database**: An external relational database (e.g., PostgreSQL or MySQL) or a distributed lock manager (e.g., ZooKeeper, Redis) maintains the table's current version pointer.
* **Advisory Locks**: Transactions acquire a row-level lock or database advisory lock on `<table_id>` during the commit phase, verify the version, insert the new metadata reference, and release the lock.
* **Comparison**: While external metadata catalogs simplify multi-store tracking, S3 native CAS (Approach 1) keeps the lakehouse architecture entirely serverless and decentralized, relying solely on object storage consistency.

## 4. The Write-Audit-Publish (WAP) Lifecycle

1. **Stage**: The writer generates data and writes files to an isolated transaction path (`_staging/txn_<UUID>/`). Readers cannot see these files because they are not referenced in any metadata manifest.
2. **Audit (Optional)**: Quality checks, validation rules, or integration tests run against the staged data.
3. **Commit (OCC)**: The commit handler attempts to atomically write `vN.json` using the CAS mechanism described above.
4. **Publish (Promotion)**: Once the metadata commit succeeds, staging files are promoted to the live directory (`data/txn_<UUID>/`) via S3 **Copy-then-Delete**. Failed transactions leave their data isolated in `_staging/` to be cleaned up later by a background Garbage Collection (Vacuum) process.

## 5. Common Pitfalls & Solutions

### 5. 1 Lost Updates (Data Overwrite)
* **Pitfall:** Reading the latest version and writing back without conditional checks (`If-None-Match`).
* **Solution:** Always enforce CAS conditional puts (`If-None-Match: "*"`) when committing metadata version files. |

### 5.2 Orphaned Staging Files
* **Pitfall:** Failed or crashed transactions leaving files in `_staging/` indefinitely, consuming storage.
* **Solution:** Implement a background Garbage Collection / Vacuum job that scans `_staging/` prefixes for transaction folders older than a retention threshold (e.g., 24 hours) and deletes them. |

### 5.3 Stale Version Reads
* **Pitfall:** Object store eventual consistency or caching returning outdated listing results during `discoverLatestVersion`.
* **Solution:** Ensure your S3 client uses consistent read listings or incorporate exponential backoff with jitter during retry loops.

### 5.4 Path Naming Mismatches
* **Pitfall:** Inconsistent use of underscores (`_`) vs. hyphens (`-`) across table names and system prefixes.
* **Solution:** Stick to strict naming conventions: hyphens for S3 buckets; underscores for system directories (`_staging/`, `metadata/`); valid SQL identifiers for table names.