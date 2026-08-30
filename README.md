## What I Built
- Extracted data from Products, Users and Carts API endpoints
- Converted JSON data into Pandas DataFrames
- Flattened nested JSON structures
- Performed null and duplicate checks
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

## Next Steps
- Pipeline orchestration (Python/Airflow automation)
- Transaction handling (COMMIT/ROLLBACK)
- Additional data quality checks (FK integrity, reject table)
- Audit/log table




## Tech Stack
- Python (pandas, requests)
- PostgreSQL
- Jupyter Notebook
