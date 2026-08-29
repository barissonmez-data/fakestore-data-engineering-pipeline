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

## Next Steps
- Pipeline orchestration (Python/Airflow automation)
- Transaction handling (COMMIT/ROLLBACK)
- Additional data quality checks (FK integrity, reject table)
- Audit/log table




## Tech Stack
- Python (pandas, requests)
- PostgreSQL
- Jupyter Notebook
