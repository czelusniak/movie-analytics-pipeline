# Movie Recommendation Pipeline — Makefile
#
# Run `make help` to see all available commands.
# Run `make <target>` to execute a specific step.
#
# Prerequisites: Python 3.11+, Docker + Docker Compose, dbt-duckdb

.PHONY: help ingest dbt-run dbt-test dbt-docs airflow-up airflow-down superset-up superset-down clean

DBT_DIR := dbt_project

help:
	@echo ""
	@echo "Movie Analytics Pipeline — Available Commands"
	@echo "=============================================="
	@echo ""
	@echo "  make ingest          Load GroupLens CSVs into DuckDB (raw layer)"
	@echo "  make dbt-run         Run all dbt models (staging → intermediate → mart)"
	@echo "  make dbt-test        Run all dbt data quality tests"
	@echo "  make dbt-docs        Generate dbt docs (lineage graph)"
	@echo ""
	@echo "  make airflow-up      Start Airflow (http://localhost:8080)"
	@echo "  make airflow-down    Stop Airflow"
	@echo "  make superset-up     Start Superset (http://localhost:8088)"
	@echo "  make superset-down   Stop Superset"
	@echo ""
	@echo "  make pipeline        Run full pipeline: ingest + dbt-run + dbt-test"
	@echo "  make clean           Remove generated files (warehouse, dbt artifacts)"
	@echo ""

# --- Data Pipeline ---

ingest:
	python ingestion/load_to_duckdb.py

dbt-run:
	cd $(DBT_DIR) && dbt run --profiles-dir . --project-dir .

dbt-test:
	cd $(DBT_DIR) && dbt test --profiles-dir . --project-dir .

dbt-docs:
	cd $(DBT_DIR) && dbt docs generate --profiles-dir . --project-dir . && dbt docs serve

pipeline: ingest dbt-run dbt-test

# --- Services ---

airflow-up:
	docker compose -f airflow/docker-compose-airflow.yml up -d
	@echo "Airflow starting... visit http://localhost:8080 (user: airflow / pass: airflow)"

airflow-down:
	docker compose -f airflow/docker-compose-airflow.yml down

superset-up:
	docker compose -f docker/docker-compose-superset.yml up -d
	@echo "Superset starting... visit http://localhost:8088 (user: admin / pass: admin)"

superset-down:
	docker compose -f docker/docker-compose-superset.yml down

# --- Cleanup ---

clean:
	rm -f data/warehouse.duckdb
	rm -rf $(DBT_DIR)/target
	rm -rf $(DBT_DIR)/dbt_packages
	@echo "Cleaned warehouse and dbt artifacts."
