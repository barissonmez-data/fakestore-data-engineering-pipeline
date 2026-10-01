# Fake Store Data Engineering Pipeline

An end-to-end batch data engineering project that extracts product, user, and cart data from the Fake Store API, transforms nested JSON responses with Python, and loads the processed data into a PostgreSQL dimensional warehouse.

Apache Airflow orchestrates the complete ETL workflow, including API availability checks, table creation, extraction, transformation, staging loads, dimension loads, and fact-table loading.

## Pipeline Overview

Fake Store API → Airflow API checks → Python transformations → PostgreSQL staging tables → dimension tables → fact table

## What I Built

* Built a single end-to-end Apache Airflow DAG for the complete ETL workflow
* Added API availability checks before starting extraction tasks
* Extracted data from the Products, Users, and Carts API endpoints
* Configured HTTP request timeouts for external API calls
* Added Airflow retry behavior for API, Python, and SQL tasks
* Added task-level execution timeouts to prevent tasks from running indefinitely
* Added operational logging for API checks, transformation record counts, and dimension and fact loading status
* Processed API JSON responses using Python data structures
* Flattened nested user, address, rating, and cart-product structures
* Filtered product and user duplicates using their IDs
* Filtered duplicate cart lines using the `cart_id + product_id` combination
* Filtered records containing null values before staging loads
* Loaded cleaned data into PostgreSQL staging tables
* Created a dedicated warehouse schema
* Built product, user, and date dimension tables
* Created surrogate keys for warehouse dimensions
* Joined staging data with dimension tables
* Loaded quantity, price, and dimension keys into the `facts_a` table
* Containerized the pipeline with Docker Compose: an Airflow service and a separate PostgreSQL warehouse service

## Current Loading Strategy

The current Airflow V1 uses a full-refresh loading strategy. During each DAG run, the staging, dimension, and fact tables are cleared and rebuilt.

Because the tables are rebuilt instead of continuously appended, repeated successful runs do not accumulate duplicate fact rows.

The current implementation executes `TRUNCATE` and `INSERT` operations separately. If an insert operation fails after a table has been cleared, the previous data is not automatically restored. Executing these operations inside a single database transaction with rollback support is planned for V2.

The original local and Jupyter implementation included a `water_mark` table and incremental loading based on:

```sql
WHERE date > watermark
```

Watermark-based incremental loading has not yet been migrated into the current Airflow DAG. It is planned for V2 after transaction handling and additional data-quality controls are implemented.

## Data Warehouse Schema

The warehouse follows a dimensional model containing product, user, and date dimensions connected to the `facts_a` table.

The `water_mark` table visible in the diagram belongs to the original local incremental-loading implementation. The current Airflow V1 follows the full-refresh strategy described above.

<img width="404" height="815" alt="PostgreSQL dimensional warehouse schema" src="https://github.com/user-attachments/assets/dc9f0f1b-4bec-454c-b4cc-9c30e905d639" />

## Airflow DAG Execution

The screenshot below shows a successful end-to-end V1 DAG run.

Airflow completed the API checks, table creation, extraction, transformation, staging loads, dimension loads, and fact-table loading.

The DAG is configured with Airflow retries, HTTP request timeouts, task-level execution timeouts, and operational logging. The screenshot confirms successful normal-path execution; failure-specific retry and timeout behavior was not separately tested during this run.

In the validated run, the final `fill_facts` task successfully loaded 14 rows into PostgreSQL.

![Successful Airflow DAG run](docs/images/airflow-dag-success.png)

## Configuration and Security

Credentials are read from a local `.env` file, which is excluded from version control. The `.env.example` file lists the required variables.

The Airflow PostgreSQL connection (`postgres`) is defined in `compose.yml` through the `AIRFLOW_CONN_POSTGRES` environment variable, so no manual connection setup is needed in the Airflow UI.

## How to Run

### Prerequisites

* Docker Desktop (includes Docker Compose)
* Git

### 1. Clone the repository

```bash
git clone https://github.com/barissonmez-data/fakestore-data-engineering-pipeline.git
cd fakestore-data-engineering-pipeline
```

### 2. Create the environment file

```bash
cp .env.example .env            # macOS / Linux
Copy-Item .env.example .env     # Windows PowerShell
```

Fill in your own values:

| Variable      | Description                                 |
|---------------|---------------------------------------------|
| `DB_USER`     | PostgreSQL user for the FakeStore warehouse |
| `DB_PASSWORD` | Password for that user                      |
| `DB_NAME`     | Database name                               |

### 3. Start the services

```bash
docker compose up --build -d
```

This starts two containers:

* **airflow**: Airflow in standalone mode, with DAGs mounted from `./dags`
* **fakestore_db**: PostgreSQL 16, storing staging, dimension and fact tables in a named volume

### 4. Check the running services

```bash
docker compose ps
```

`fakestore_db` should show `(healthy)`. Airflow starts only after PostgreSQL is healthy, so on the first startup it may take about 20–30 seconds before the Airflow UI is available.

### 5. Get the Airflow admin password

```bash
docker compose logs airflow | grep Password       # macOS / Linux
docker compose logs airflow | findstr Password    # Windows
```

A new password is generated whenever the Airflow container is recreated.

### 6. Open Airflow

Go to http://localhost:8082 and log in with user `admin` and the password from step 5.

### 7. Run the pipeline

Enable and trigger the `fake_store_pipeline` DAG from the Airflow interface. All tasks should finish successfully.

Task progress, retries, execution status, and operational logs can be monitored directly from the DAG view.

Optional: verify the warehouse tables:

```bash
docker compose exec fakestore_db psql -U <DB_USER> -d <DB_NAME> -c "\dt fakestore_warehouse.*"
```

### 8. Stop the services

* `docker compose stop`: stops the containers and keeps them
* `docker compose down`: removes the containers; database data in the volume is kept

## Design Decisions

* **Two services, two sources.** Airflow is built from a custom `Dockerfile` because the DAG needs extra packages (`requests`, PostgreSQL provider). PostgreSQL uses the official `postgres:16` image and is configured only through environment variables.
* **Separate warehouse database.** FakeStore staging, dimension and fact tables live in `fakestore_db`, not in Airflow's own metadata database.
* **DAGs via bind mount.** `./dags` is mounted into the Airflow container, so DAG changes are picked up immediately without rebuilding the image. A rebuild (`docker compose up -d --build`) is only needed when `Dockerfile` or `requirements.txt` changes.
* **PostgreSQL data in a named volume.** Tables survive `docker compose down` because they are stored in a volume, not inside the container.
* **Connection as code.** The Airflow connection is defined with `AIRFLOW_CONN_POSTGRES` in `compose.yml` instead of the UI, so it survives container recreation and needs no manual setup.
* **Container networking.** Airflow reaches PostgreSQL by its service name on the Compose network (`fakestore_db:5432`). PostgreSQL is not exposed to the host; only the Airflow UI is published (`8082:8080`).
* **Pinned image versions.** Images use explicit tags (e.g. `postgres:16`) so the stack does not change when a new version is released.
* **Secrets outside the repository.** Credentials are read from `.env` (git-ignored); `.env.example` documents the required variables.
* **Healthcheck-based startup order.** PostgreSQL has a `pg_isready` healthcheck, and Airflow uses `depends_on` with `condition: service_healthy`. Airflow therefore starts only after the database accepts connections, not just after its container is running.

### Known Limitations

* Airflow runs in `standalone` mode, which is intended for local development.
* The Airflow admin password is regenerated whenever the container is recreated.
* Airflow's own metadata (run history) is not persisted across `docker compose down`.

## V1 Scope

The completed V1 includes:

* End-to-end Airflow orchestration
* API availability checks
* Products, users, and carts extraction
* Nested JSON transformation
* Basic duplicate and null filtering
* PostgreSQL staging loads
* Product, user, and date dimensions
* Fact-table loading
* HTTP request timeouts
* Airflow task retries
* Task-level execution timeouts
* Basic operational logging
* Docker-based local execution
* PostgreSQL healthcheck with ordered service startup
* Full-refresh loading

Advanced reliability, data-quality, testing, and incremental-loading features are planned for V2.

## Next Steps

* Define the fact-table grain explicitly and retain `cart_id` as a degenerate dimension
* Execute `TRUNCATE + INSERT` operations inside a single transaction with rollback support
* Add source-to-fact row-count reconciliation
* Detect and log records lost because of missing dimension matches
* Add foreign-key integrity checks between fact and dimension tables
* Expand structured logging to include extracted, accepted, rejected, and duplicate row counts
* Add audit and rejected-record tables
* Migrate watermark-based incremental loading into the Airflow DAG
* Add unit tests for transformation functions
* Add integration tests for warehouse loading
* Add continuous integration checks
* Add Airflow failure notifications and alerting

## Tech Stack

* Python
* Apache Airflow
* PostgreSQL
* SQL
* requests
* Docker
* Docker Compose
* pandas and Jupyter Notebook for the original local implementation
* Git and GitHub
