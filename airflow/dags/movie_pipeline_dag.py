"""
Movie Recommendation Pipeline — Airflow DAG
============================================

What this file teaches you:
  - How to define a DAG (Directed Acyclic Graph) in Airflow
  - How to chain tasks with >> (dependency operator)
  - How to run shell commands from Airflow using BashOperator
  - How to configure scheduling with cron expressions
  - How to set retries and timeouts per task

DAG flow:
    ingest_raw_data → dbt_run → dbt_test

To trigger manually:
  1. Open Airflow at http://localhost:8080
  2. Find the DAG "movie_recommendation_pipeline"
  3. Click the "play" button (Trigger DAG)
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# --- Default arguments ---
# These apply to ALL tasks in the DAG unless overridden per-task.
# In production, you'd set:
#   - email_on_failure: True  (with a real email)
#   - on_failure_callback: a Slack/PagerDuty hook
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,       # Each run is independent
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,                   # Retry once before failing
    "retry_delay": timedelta(minutes=5),
}

# --- DAG definition ---
# schedule_interval: when to run automatically
#   "@daily"    = every day at midnight
#   "0 6 * * *" = every day at 06:00 UTC (cron syntax)
#   None        = manual triggers only (good while developing)
#
# catchup=False: don't run for all past dates when you first enable the DAG.
# Without this, Airflow would try to backfill from start_date — usually not what you want.
with DAG(
    dag_id="movie_recommendation_pipeline",
    description="Ingest GroupLens CSVs → dbt transform → dbt test",
    default_args=default_args,
    start_date=datetime(2024, 1, 1), # catchup false means this is just a placeholder; it won't backfill
    schedule_interval=None,         # Change to "@daily" when you're ready to automate
    catchup=False,
    tags=["movies", "dbt", "batch"],
) as dag:

    # --- Task 1: Ingest raw CSVs into DuckDB ---
    # BashOperator runs a shell command in the Airflow worker container.
    # The working directory is set to /opt/airflow (inside the Docker container).
    # We navigate to the project root and run the ingestion script.
    #
    # LEARNING POINT: In production, you'd use a PythonOperator or a custom operator
    # instead of BashOperator for Python scripts. BashOperator is simpler to start with.
    ingest_raw_data = BashOperator(
        task_id="ingest_raw_data",
        bash_command="cd /opt/app && python ingestion/load_to_duckdb.py",
        # How long to wait before timing out this task
        execution_timeout=timedelta(minutes=10),
    )

    # --- Task 2: Run dbt transformations ---
    # `dbt run` executes all models in dependency order (staging → intermediate → mart).
    # --profiles-dir: tells dbt where to find profiles.yml
    # --project-dir: tells dbt where to find dbt_project.yml
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            "cd /opt/app/dbt_project "
            "&& dbt run "
            "--profiles-dir . "
            "--project-dir ."
        ),
        execution_timeout=timedelta(minutes=15),
    )

    # --- Task 3: Run dbt data quality tests ---
    # `dbt test` runs all tests defined in schema.yml files.
    # If any test fails, Airflow marks this task (and the DAG run) as FAILED.
    # This is your automated data quality gate.
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            "cd /opt/app/dbt_project "
            "&& dbt test "
            "--profiles-dir . "
            "--project-dir ."
        ),
        execution_timeout=timedelta(minutes=10),
    )

    # --- Task dependencies ---
    # >> means "run after".
    # This defines the execution order:
    #   1. ingest_raw_data must succeed
    #   2. THEN dbt_run
    #   3. THEN dbt_test
    ingest_raw_data >> dbt_run >> dbt_test

    dbt_docs_generate = BashOperator(
        task_id="dbt_docs_generate",
        bash_command=(
            "cd /opt/app/dbt_project "
            "&& dbt docs generate "
            "--profiles-dir . "
            "--project-dir ."
        ),
        execution_timeout=timedelta(minutes=10),
    )

    ingest_raw_data >> dbt_run >> dbt_test >> dbt_docs_generate
