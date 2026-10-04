# Fake Store Data Engineering Pipeline

An end-to-end batch ETL pipeline that extracts product, user, and cart data from the Fake Store API, transforms and validates the data with Python, and loads it into a PostgreSQL dimensional warehouse.

Apache Airflow orchestrates the workflow, while Docker Compose runs Airflow and PostgreSQL as separate local services.

![Successful Airflow DAG run](docs/images/airflow-dag-success.png)

---

## Main Features

- Extracts products, users, and carts from the Fake Store API
- Checks API availability before processing
- Flattens nested JSON structures
- Removes duplicate product and user records by ID
- Removes duplicate cart lines using `cart_id + product_id`
- Rejects records containing null values
- Stores rejected records in `reject_table` with reason, source, timestamp, and record data
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

- `product_dim`
- `user_dim`
- `date_dim`
- `facts_a`

Staging tables:

- `stg_products`
- `stg_user`
- `stg_carts`

Rejected records are stored in `reject_table`.

The current Airflow implementation uses a **full-refresh loading strategy**.

---

## Reliability and Data Quality

### Transactional Loads

Each staging, dimension, and fact-table load runs inside its own PostgreSQL transaction.

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

### Row-Count Reconciliation

For staging loads, the pipeline compares the number of accepted records with the number of rows actually loaded into PostgreSQL.

The fact-table load also compares staged cart lines with loaded fact rows.

If the counts do not match, the task fails and the current transaction is rolled back.

### Full-Refresh Idempotency

The pipeline was run repeatedly against the same source data and warehouse row counts remained stable between successful runs.

This prevents duplicate rows from accumulating across repeated full-refresh executions.

---

## Docker Setup

The project runs with two main services:

### Airflow

Airflow runs the DAG and is built from the project `Dockerfile`.

The local `./dags` directory is bind-mounted into the container, so DAG changes do not require rebuilding the Airflow image.

The Airflow UI is available at `http://localhost:8082`.

### PostgreSQL

The warehouse runs on PostgreSQL 16 in a separate container.

Warehouse data is stored in a named Docker volume so it survives normal container recreation.

Airflow connects to PostgreSQL internally through `fakestore_db:5432`.

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
- **Row-count validation:** silent data loss is detected instead of ignored

---

## Current V2 Progress

Completed:

- Transaction-based database loads
- Rollback on failure
- Error handling
- Row-count reconciliation
- Reject table and reject handling
- Operational logging
- Full-refresh idempotency validation

Next:

1. Audit / pipeline run table
2. Referential integrity and stronger data-quality checks
3. Incremental loading
4. Unit and integration tests
5. CI
6. Failure notifications

---

## Known Limitations

- Airflow runs in `standalone` mode for local development
- The current Airflow pipeline uses full-refresh loading
- Historical run metrics are not yet stored in an audit table
- Referential-integrity checks are not yet fully implemented
- Automated tests and CI are not yet implemented
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
