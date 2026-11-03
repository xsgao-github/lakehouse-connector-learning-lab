# Lakehouse Connector Learning Lab

A hands-on, modular Java learning lab designed to build a custom lakehouse query and storage engine from scratch—learning metadata catalogs, manifest lists, parquet scanning, and predicate pushdown without heavy enterprise query frameworks.

The scope is strictly centered on core lakehouse mechanics: file-based catalog management, metadata parsing, atomic commits via Optimistic Concurrency Control (OCC), and columnar data reading (bypassing complex query engine data-type translation layers). **External catalog services (such as Hive Metastore or Iceberg REST catalogs) are omitted in favor of a pure file-based storage catalog.**

## 🚀 Quick Start

### Prerequisites
- [Docker Desktop](https://www.docker.com/products/docker-desktop/)
- [Java 17+](https://adoptium.net/)
- [Maven](https://maven.apache.org/download.cgi)
- [Python 3.x](https://www.python.org/downloads/)

### Setup & Run
1. **Start Local MinIO:**
   ```bash
   docker compose up -d
   ```

2. **Set .venv** You will need it to populate test data later.
   ```bash
   python -m venv .venv
   ./.venv/Scripts/Activate.ps1 
   python -m pip install --upgrade pip
   pip install -r ./scripts/requirements.txt
   ```

3. **Build and Test**
   ```bash
   # (optionL) remove old data
   rm -r .\minio_data\warehouse\tables\
   mvn clean test
   # append -DkeepData=true to prevent tests from cleanup test data from Minio after run
   # mvn clean test -Dtest=OCCCommitHandlerTest -DkeepData=true
   ```

### Minio Login

- URL: `http://localhost:9000` (API), http://localhost:9001 (web UI)
- Login user/password: `admin/password123`


### Object Store File Structure

```plaintext
warehouse/
└── tables/
    └── orders/
        ├── metadata/
        │   ├── v1.json
        │   └── v2.json
        ├── _staging/
        │   └── txn_f47ac10b-58cc/
        │       └── date=2026-11-04/
        │           └── part-00001.parquet
        └── data/
            ├── date=2026-11-04/
            │   └── part-00001.parquet
            └── date=2026-11-05/
                └── part-00002.parquet
```

## 📂 Project Structure

- **`utils/`**: S3 client factory, and utilities.
- **`catalog/`**: Metadata models, JSON parsers, and schema evolution handlers (Module 01).
- **`concurrency/`**: OCC retry loops and commit conflict handlers for atomic writes (Module 02).
- **`planner/`**: Split generators and partition pruning filters (Module 03).
- **`reader/`**: Parquet file scanners and columnar projection pushdown (Module 04).
- **`resilience/`**: S3 error handlers, retry policies, and idempotency guarantees (Module 05).

## 🗺️ Learning Roadmap
This project is organized as a series of modules. Each module introduces a new Lakehouse concept, implements it in code, and documents the findings. Modules may depends on previous modules, so you need to follow the sequence bellow.

- [x] **Module 0**: Getting Started (Baseline) -> [docs/module-00-setup.md](docs/module-00-setup.md)
- [ ] **Module 1**: Storage-Centric Catalog & Schema Evolution -> [docs/module-01.md](docs/module-01.md) (Tracks table state via versioned JSON metadata files directly on object storage)
- [ ] **Module 2**: Optimistic Concurrency Control (OCC) & WAP Commits -> [docs/module-02.md](docs/module-02.md) (Prevents race conditions using staging prefixes and atomic retry loops)
- [ ] **Module 3**: Split Generation & Partition Pruning -> [docs/module-03.md](docs/module-03.md) (Evaluates filter predicates against partition folder paths to skip irrelevant data)
- [ ] **Module 4**: Columnar Projection & Predicate Pushdown -> [docs/module-04.md](docs/module-04.md) (Reads only requested columns and pushes down filtering logic)
- [ ] **Module 5**: Robust Error Handling & S3 Idempotency -> [docs/module-05.md](docs/module-05.md) (Handles transient network failures, stale conflicts, and bulletproof recovery)
