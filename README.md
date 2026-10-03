# Fake Store Data Engineering Pipeline

An end-to-end batch ETL pipeline that extracts product, user, and cart data from the Fake Store API, transforms nested JSON with Python, and loads it into a PostgreSQL dimensional warehouse.

Apache Airflow orchestrates the workflow, while Docker Compose runs Airflow and PostgreSQL locally as separate services.

```text
Fake Store API → API Checks → Python Transformations → Staging → Dimensions → Fact Table
```

![Successful Airflow DAG run](docs/images/airflow-dag-success.png)

*Validated end-to-end DAG run with 21 successful tasks, from API checks to the final fact-table load.*

---

## What the Pipeline Does

- Extracts products, users, and carts from the Fake Store API
- Checks API availability before extraction starts
- Flattens nested user, address, rating, and cart-product data
- Removes duplicate product and user records by ID
- Removes duplicate cart lines using `cart_id + product_id`
- Filters records containing null values before staging
- Loads cleaned data into PostgreSQL staging tables
- Builds product, user, and date dimensions
- Uses surrogate keys in the dimensional model
- Loads the final fact table
- Uses retries, HTTP timeouts, task execution timeouts, and operational logging
- Runs warehouse loads inside PostgreSQL transactions with rollback support
- Runs locally through Docker Compose

---

## Data Warehouse Schema

The warehouse follows a dimensional model with product, user, and date dimensions connected to the `facts_a` table.

<img width="404" height="815"
     alt="PostgreSQL dimensional warehouse schema"
     src="https://github.com/user-attachments/assets/dc9f0f1b-4bec-454c-b4cc-9c30e905d639" />

The `water_mark` table shown in the diagram belongs to the original local incremental-loading implementation. The current Airflow pipeline still uses full-refresh loading.

---

## Loading Strategy

The current Airflow pipeline uses a **full-refresh strategy**.

Each DAG run clears and rebuilds the staging, dimension, and fact tables using the latest source data.

Repeated successful runs therefore rebuild the warehouse instead of continuously appending duplicate rows.

### Atomic Loads

`TRUNCATE` and `INSERT` are executed inside the same PostgreSQL transaction.

On success:

```text
TRUNCATE
→ INSERT
→ COMMIT
```

If the insert fails:

```text
TRUNCATE
→ INSERT fails
→ ROLLBACK
```

This prevents a table from being left empty if `TRUNCATE` succeeds but the following insert fails.

The transaction is committed only when the load finishes successfully. On failure, the transaction is rolled back and the exception is raised again.

This pattern is used for the staging, dimension, and fact-table loads.

---

## Idempotency Validation

The pipeline was run multiple times against the same source data.

A `UNION ALL` query was used to compare row counts across all seven warehouse tables:

- 3 staging tables
- 3 dimension tables
- 1 fact table

The row counts remained unchanged after repeated successful DAG runs.

This confirms that the current full-refresh implementation does not accumulate duplicate rows between executions.

---

## Docker Setup

The project runs with two main Docker Compose services:

### Airflow

Airflow runs the ETL workflow and is built from the project's custom `Dockerfile`.

The local DAG directory is bind-mounted into the container:

```text
./dags → /opt/airflow/dags
```

Because of the bind mount, DAG code changes can be picked up without rebuilding the Airflow image.

A rebuild is mainly required when the `Dockerfile` or `requirements.txt` changes.

### PostgreSQL

The warehouse runs in a separate PostgreSQL 16 container.

PostgreSQL data is stored in a named Docker volume, so the warehouse survives normal container recreation and:

```bash
docker compose down
```

Airflow connects to PostgreSQL through the Docker Compose network using:

```text
fakestore_db:5432
```

PostgreSQL does not need to expose a host port for communication with Airflow.

Only the Airflow UI is published to the host:

```text
8082:8080
```

---

## PostgreSQL Health Check

PostgreSQL uses `pg_isready` to check whether the database is ready to accept connections.

Airflow depends on PostgreSQL with:

```yaml
depends_on:
  fakestore_db:
    condition: service_healthy
```

During Docker Compose startup, Airflow waits until PostgreSQL reports a healthy status before starting.

This avoids Airflow attempting to connect while PostgreSQL is still initializing.

---

## Configuration

Database credentials are stored in a local `.env` file, which is excluded from Git.

The required variables are documented in `.env.example`:

```text
DB_USER
DB_PASSWORD
DB_NAME
```

The Airflow PostgreSQL connection is configured in `compose.yml` through:

```text
AIRFLOW_CONN_POSTGRES
```

This means the PostgreSQL connection does not need to be created manually through the Airflow UI.

---

## How to Run

### Prerequisites

- Docker Desktop
- Docker Compose
- Git

### 1. Clone the repository

```bash
git clone https://github.com/barissonmez-data/fakestore-data-engineering-pipeline.git
cd fakestore-data-engineering-pipeline
```

### 2. Create the environment file

macOS / Linux:

```bash
cp .env.example .env
```

Windows PowerShell:

```powershell
Copy-Item .env.example .env
```

Add your own values for:

```text
DB_USER
DB_PASSWORD
DB_NAME
```

### 3. Start the services

```bash
docker compose up --build -d
```

Check their status:

```bash
docker compose ps
```

`fakestore_db` should report `healthy`.

### 4. Get the Airflow password

macOS / Linux:

```bash
docker compose logs airflow | grep Password
```

Windows:

```powershell
docker compose logs airflow | findstr Password
```

### 5. Open Airflow

Open:

```text
http://localhost:8082
```

Log in with:

```text
username: admin
```

and the generated Airflow password.

Enable and trigger:

```text
fake_store_pipeline
```

### 6. Optional warehouse check

```bash
docker compose exec fakestore_db psql -U <DB_USER> -d <DB_NAME> -c "\dt fakestore_warehouse.*"
```

### 7. Stop the services

Stop the containers without removing them:

```bash
docker compose stop
```

Or remove the containers:

```bash
docker compose down
```

The PostgreSQL data remains in the named volume.

Be careful with:

```bash
docker compose down -v
```

because `-v` also deletes the PostgreSQL volume.

---

## Design Decisions

### Separate Airflow and Warehouse Services

Airflow and PostgreSQL run as separate services.

The Fake Store warehouse is kept separate from Airflow's own metadata database.

### Connection as Code

The PostgreSQL connection is defined through `AIRFLOW_CONN_POSTGRES` in `compose.yml` instead of being configured manually in the Airflow UI.

This makes the local environment easier to recreate.

### Named Volume for PostgreSQL

Warehouse data is stored outside the PostgreSQL container in a named volume.

This allows the data to survive normal container recreation.

### Bind Mount for DAG Development

The local `./dags` directory is mounted directly into the Airflow container.

DAG changes therefore do not require an image rebuild.

### Health-Based Startup

Airflow waits for PostgreSQL to become ready before starting during Docker Compose startup.

### Atomic Database Loads

`TRUNCATE` and `INSERT` run inside the same transaction.

Successful loads are committed, while failed loads are rolled back.

This prevents failed refreshes from leaving warehouse tables empty.

---

## Known Limitations

- Airflow currently runs in `standalone` mode for local development
- The Airflow version currently uses full-refresh loading only
- Data-quality checks currently focus on duplicate and null filtering
- Rejected records are not stored in a dedicated table yet
- Complete source-to-target row-count reconciliation is not implemented yet
- Automated unit and integration tests are not implemented yet
- No CI pipeline yet
- No failure notification or monitoring system yet

---

## Next Steps

1. **Row-count reconciliation**  
   Track extracted, accepted, loaded, and rejected row counts to detect silent data loss.

2. **Reject table**  
   Store rejected records together with the rejection reason and pipeline stage.

3. **Audit table**  
   Store per-run and per-table information such as row counts, status, and timestamps.

4. **Integrity checks**  
   Detect fact rows with missing or invalid dimension references.

5. **Incremental loading**  
   Bring the existing watermark-based approach into the Airflow DAG.

6. **Testing**  
   Add unit tests for transformation logic and integration tests for PostgreSQL loading.

7. **Continuous Integration**  
   Run automated tests when changes are pushed to the repository.

8. **Failure notifications**  
   Add Airflow alerts for failed pipeline runs.

---

## Tech Stack

- Python
- SQL
- Apache Airflow
- PostgreSQL 16
- Docker
- Docker Compose
- requests
- pandas
- Git
- GitHub
