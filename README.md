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

Local Airflow configuration, logs, environment files, and generated secrets are excluded from version control.

The required PostgreSQL connection must be configured locally in Airflow before triggering the DAG. Credentials and generated secrets should never be committed to the repository.

## How to Run

### Prerequisites

* Docker
* Docker Compose
* Git

### 1. Clone the repository

```bash
git clone https://github.com/barissonmez-data/fakestore-data-engineering-pipeline.git
cd fakestore-data-engineering-pipeline
```

### 2. Start the services

```bash
docker compose up --build -d
```

### 3. Check the running services and ports

```bash
docker compose ps
```

Open the Airflow web interface using the host port displayed for the Airflow service.

### 4. Verify the PostgreSQL connection

Before triggering the DAG, confirm that Airflow contains a PostgreSQL connection with the following connection ID:

```text
postgres
```

### 5. Run the pipeline

Enable and trigger the `fake_store_pipeline` DAG from the Airflow interface.

Task progress, retries, execution status, and operational logs can be monitored directly from the DAG view.

### 6. Stop the services

```bash
docker compose down
```

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
