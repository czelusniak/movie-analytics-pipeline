from pathlib import Path
import duckdb

# 1. Define relative paths
script_dir = Path(__file__).parent.parent  # go up one level from ingestion/ to project root
raw_dir = script_dir / "data" / "raw"
db_path = script_dir / "data" / "warehouse.duckdb"

print(f"Script: {script_dir}")
print(f"CSVs: {raw_dir}")
print(f"DB: {db_path}")

# 2. Creating DuckDB connection
with duckdb.connect(str(db_path)) as conn:
    # 3. Create schema if not exists
    conn.execute("CREATE SCHEMA IF NOT EXISTS raw")

    # 4. Load CSV into DuckDB
    csv_files = ['movies.csv', 'user_rating_history.csv', 'ratings_for_additional_users.csv']
    for file in csv_files:
        csv_file = raw_dir / file
        table_name = file.replace('.csv', '')
        conn.execute(f"""
        CREATE OR REPLACE TABLE raw.{table_name} AS
        SELECT * FROM read_csv_auto('{csv_file}', nullstr='NA', sample_size=-1)
    """)
        print(f"Loaded {table_name}")
