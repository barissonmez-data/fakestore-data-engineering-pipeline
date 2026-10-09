# Fake Store Data Engineering Pipeline

An end-to-end batch ETL pipeline that extracts product, user, and cart data from the Fake Store API, transforms and validates the data with Python, and loads it into a PostgreSQL dimensional warehouse.

Apache Airflow orchestrates the workflow, while Docker Compose runs Airflow and PostgreSQL as separate local services.

![Successful Airflow DAG run](docs/images/airflow-dag-success.png)

---

## Pipeline Flow

```text
Fake Store API
      ↓
Apache Airflow
      ↓
Extract → Transform → Validate
      ↓
PostgreSQL Staging
      ↓
Dimensions and Fact Table
```

---

## Main Features

- Extracts products, users, and carts from the Fake Store API
- Checks API availability before processing
- Flattens nested JSON structures
- Removes duplicate product and user records by ID
- Removes duplicate cart lines using `cart_id + product_id`
- Rejects records with null required fields or identifiers
- Stores rejected records in `reject_table`
- Records pipeline-run metrics in `audit_table`
- Loads cleaned data into PostgreSQL staging tables
- Builds product, user, and date dimensions
- Loads the final fact table using surrogate keys
- Performs row-count reconciliation during staging and fact loading
- Uses retries, HTTP timeouts, task execution timeouts, and logging
- Uses PostgreSQL transactions with rollback on failure
- Runs locally with Docker Compose

---

## Data Warehouse

The warehouse uses a dimensional model with:

- `fakestore_warehouse.product_dim`
- `fakestore_warehouse.user_dim`
- `fakestore_warehouse.date_dim`
- `fakestore_warehouse.facts_a`

Staging tables:

- `public.stg_products`
- `public.stg_user`
- `public.stg_carts`

Operational tables:

- `public.reject_table`
- `public.audit_table`

The current Airflow implementation uses a full-refresh loading strategy.

---

## Reliability and Data Quality

### Transactional Loads

Each staging, dimension, and fact-table load runs inside a PostgreSQL transaction.

If a load succeeds, the transaction is committed. If an error occurs, the transaction is rolled back and the exception is raised again so Airflow can mark the task as failed.

This prevents failed refresh operations from leaving an individual table partially updated.

### Reject Handling

Null and duplicate records are stored in `reject_table` instead of being silently discarded.

The reject table stores:

- record ID
- rejected record data
- rejection reason
- rejection timestamp
- source

Reject handling is implemented for products, users, and cart lines.

### Audit and Run Metrics

The `audit_table` records pipeline metrics for each source:

- execution timestamp
- source name
- extracted record count
- accepted record count
- rejected record count
- pipeline status

This makes successful and rejected records inspectable after each run.

### Row-Count Reconciliation

For staging loads, the pipeline compares the number of accepted records with the number of rows actually loaded into PostgreSQL.

The fact-table load also compares staged cart lines with loaded fact rows.

If the counts do not match, the task fails and the current transaction is rolled back.

### Full-Refresh Idempotency

The pipeline was run repeatedly against the same source data and warehouse row counts remained stable between successful runs.

This prevents duplicate rows from accumulating across repeated full-refresh executions.

---

## Docker Setup

The project runs with two main services.

### Airflow

Airflow runs the DAG and is built from the project `Dockerfile`.

The local `./dags` directory is bind-mounted into the container, so DAG changes do not require rebuilding the Airflow image.

The Airflow UI is available at:

```text
http://localhost:8082
```

### PostgreSQL

The warehouse runs on PostgreSQL 16 in a separate container.

Warehouse data is stored in a named Docker volume so it survives normal container recreation.

Airflow connects to PostgreSQL internally through:

```text
fakestore_db:5432
```

A `pg_isready` health check ensures PostgreSQL is ready before Airflow starts.

---

## Configuration

Database credentials are stored in a local `.env` file, which is excluded from Git.

Required variables:

- `DB_USER`
- `DB_PASSWORD`
- `DB_NAME`

The required variables are documented in `.env.example`.

The Airflow PostgreSQL connection is configured through `AIRFLOW_CONN_POSTGRES` in `compose.yml`, so it does not need to be created manually in the Airflow UI.

---

## Run Locally

Clone the repository:

```bash
git clone https://github.com/barissonmez-data/fakestore-data-engineering-pipeline.git
cd fakestore-data-engineering-pipeline
```

Create the environment file:

```bash
cp .env.example .env
```

On Windows PowerShell:

```powershell
Copy-Item .env.example .env
```

Start the services:

```bash
docker compose up --build -d
```

Check their status:

```bash
docker compose ps
```

Open Airflow at `http://localhost:8082` and trigger `fake_store_pipeline`.

Stop the project with:

```bash
docker compose down
```

PostgreSQL data remains in the named volume. Avoid `docker compose down -v` unless you intentionally want to delete the database volume.

---

## Design Decisions

- **Separate Airflow and PostgreSQL services:** keeps orchestration separate from warehouse storage
- **Connection as code:** the PostgreSQL connection is configured through Docker Compose
- **Named PostgreSQL volume:** warehouse data survives container recreation
- **Bind-mounted DAG directory:** DAG changes do not require rebuilding the image
- **Health-based startup:** Airflow waits until PostgreSQL is ready
- **Transactional loads:** failed database loads are rolled back
- **Reject handling:** invalid records remain inspectable
- **Audit table:** pipeline-run metrics are stored for later inspection
- **Row-count validation:** silent data loss is detected instead of ignored

---

## Current V2 Progress

Completed:

- Transaction-based database loads
- Rollback on database failure
- Null and duplicate validation
- Reject table and rejected-record handling
- Audit table with extracted, accepted, rejected, and status metrics
- Row-count reconciliation
- Operational logging
- HTTP and task execution timeouts
- Retry configuration
- Full-refresh idempotency validation
- Full API pipeline validation
- Airflow mock validation for null and duplicate records

The final DAG configuration is:

```python
TEST_MODE = False
```

This runs the main API-to-warehouse pipeline. Mock mode was used only for local validation.

---

## Validation Evidence

### Full API validation

| Source | Extracted | Accepted | Rejected | Status |
|---|---:|---:|---:|---|
| users | 10 | 10 | 0 | success |
| products | 20 | 20 | 0 | success |
| carts | 14 | 14 | 0 | success |

### Mock validation

| Source | Extracted | Accepted | Rejected |
|---|---:|---:|---:|
| users | 3 | 1 | 2 |
| products | 3 | 1 | 2 |

Mock rejection reasons included null IDs and duplicate records.

### Final database verification

| Table | Rows |
|---|---:|
| `public.stg_user` | 10 |
| `public.stg_products` | 20 |
| `public.stg_carts` | 14 |
| `fakestore_warehouse.user_dim` | 10 |
| `fakestore_warehouse.product_dim` | 20 |
| `fakestore_warehouse.facts_a` | 14 |
| `fakestore_warehouse.date_dim` | 4 |

The mock validation is an Airflow integration and behavior validation. Formal standalone `pytest` tests have not been implemented yet.

---

## Future Improvements

- Referential-integrity and missing-dimension checks
- Incremental loading with a watermark
- Formal unit and integration tests with `pytest`
- Continuous integration with GitHub Actions
- Automated failure notifications

---

## Known Limitations

- Airflow runs in `standalone` mode for local development
- The current pipeline uses full-refresh loading
- Referential-integrity checks are not yet fully implemented
- Formal standalone `pytest` tests and CI are not yet implemented
- Failure notifications are not yet implemented

---

## Tech Stack

- Python
- SQL
- Apache Airflow
- PostgreSQL 16
- Docker
- Docker Compose
- requests
- Git
- GitHub
