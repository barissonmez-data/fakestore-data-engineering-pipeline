## What I Built
- Extracted data from Products, Users and Carts API endpoints
- Converted JSON data into Pandas DataFrames
- Flattened nested JSON structures
- Loaded cleaned data into PostgreSQL staging tables
- Created separate staging and warehouse schemas
- Built product, user and date dimensions
- Created surrogate keys
- Joined staging data with dimensions
- Loaded the resulting data into the fact table
- Added SQL data quality checks
- Built a watermark table for incremental loading
- Implemented incremental load logic (WHERE date > watermark)
- Idempotency: The pipeline was made resilient to repeated runs, ensuring that re-executing it does not create duplicate rows in the facts_a table. This is achieved by updating the last_load value in the water_mark table after each INSERT to reflect the most recent processed date — so subsequent runs only pick up new, previously unprocessed data.
- Orchestrated the staging pipeline (table creation, API availability checks, extraction, and processing) using Apache Airflow
- Added duplicate checks (id-based for products/users, composite cart_id + product_id key for carts) to catch repeated records within a single API response
- Added null checks to prevent incomplete records from propagating to later stages of the pipeline

## Next Steps
- Migrate dimension and fact table population into Airflow (currently still handled outside the DAG)
- Transaction handling (COMMIT/ROLLBACK)
- Additional data quality checks (FK integrity, reject table)
- Audit/log table

## Tech Stack
- Python (pandas, requests)
- Apache Airflow
- PostgreSQL
- Jupyter Notebook

<img width="404" height="815" alt="postgres - postgres - fakestore_warehouse" src="https://github.com/user-attachments/assets/dc9f0f1b-4bec-454c-b4cc-9c30e905d639" />
