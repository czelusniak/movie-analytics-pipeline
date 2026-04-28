# Pipeline macro flow — understanding what happens

## The 2 main misconceptions

### Misconception 1: "a container starts up when the pipeline needs to run"

**Wrong.** Airflow containers run **continuously**, 24/7.

Think of them as a server you start once (`docker compose up`) and leave running. The `airflow-scheduler` container is always alive, polling constantly to check whether any DAG is due to run.

Containers only stop if you:
- Manually shut them down (`docker compose down`)
- Restart the machine
- A fatal error occurs

### Misconception 2: "the project is uploaded into the container"

**Wrong.** The project is **never copied** into the container.

What exists is a **volume** — a shared window between your machine and the container. When the container looks at `/opt/app`, it is seeing, in real time, the `~/dev/netflix-data` folder on your machine. If you edit a file in VS Code, the container sees the change immediately.

```
YOUR MACHINE                   CONTAINER
~/dev/netflix-data/   ←──→   /opt/app/
  ├── ingestion/                ├── ingestion/
  ├── dbt_project/              ├── dbt_project/
  └── data/                     └── data/
        ↑ SAME FILES, no copy
```

---

## The real architecture — what is always running

When you run `docker compose -f airflow/docker-compose-airflow.yml up -d`, three containers come up and stay **permanently alive**:

### 1. `postgres`
Airflow's own metadata database. Stores:
- Which DAGs exist and their structure
- Execution history (every run of every task)
- Current status (running, failed, success, waiting)
- Airflow configuration (connections, variables)

**Important:** this postgres has nothing to do with your movie data. It is Airflow's "memory brain".

### 2. `airflow-scheduler`
The heart of Airflow. Runs in an infinite loop:

```
every few seconds:
  1. reads .py files in /opt/airflow/dags/
  2. checks: should any DAG be running now?
     - did a schedule trigger fire?
     - did someone click "Trigger" in the UI?
  3. if yes, creates task instances in postgres
  4. executes the tasks (with LocalExecutor, runs in the scheduler process itself)
```

### 3. `airflow-webserver`
The web UI at `http://localhost:8080`. Does nothing in the pipeline — it only displays the state stored in postgres as a visual interface.

### One temporary container: `airflow-init`
Runs **once** the first time you do `docker compose up`. Creates the postgres tables and the admin user. Then exits. It does not stay running.

---

## The step-by-step flow, from a temporal perspective

### Moment T0 — you start everything

```bash
docker compose -f airflow/docker-compose-airflow.yml up -d
```

What happens:
1. Docker pulls the images (first time only)
2. Starts `postgres` and waits for it to become healthy
3. Runs `airflow-init` (creates tables + admin user) and exits
4. Starts `airflow-webserver` and `airflow-scheduler`
5. **All three stay running. Forever. Until you tell them to stop.**

### Moment T1 — you open the browser

`http://localhost:8080` shows the list of available DAGs. `movie_recommendation_pipeline` appears because the scheduler automatically read `airflow/dags/movie_pipeline_dag.py`.

### Moment T2 — something triggers the DAG

Could be:
- **Manual:** you click "Trigger DAG" in the UI
- **Schedule:** the DAG has `schedule="@daily"` and midnight arrived
- **Sensor:** something detected a change in an external source (not used here)

### Moment T3 — the DAG executes

The scheduler sees it needs to run. Creates task instances in postgres:

```
1. ingest_raw_data    (status: pending)
2. dbt_run            (status: pending)
3. dbt_test           (status: pending)
4. dbt_docs_generate  (status: pending)
```

And starts executing **in the order defined by the dependencies**:

#### Task 1: `ingest_raw_data`
- Command: `cd /opt/app && python ingestion/load_to_duckdb.py`
- The scheduler runs this command inside its own container
- Python reads CSVs from `/opt/app/data/raw/` (which is your `~/dev/netflix-data/data/raw/`)
- Writes to `/opt/app/data/warehouse.duckdb` (which is your `~/dev/netflix-data/data/warehouse.duckdb`)
- Status in postgres changes to `success` or `failed`

#### Task 2: `dbt_run`
- Only runs if task 1 finished with `success`
- Command: `cd /opt/app/dbt_project && dbt run --profiles-dir . --project-dir .`
- Runs all models: 3 staging → 2 intermediate → 5 marts
- Each model writes to `warehouse.duckdb`

#### Task 3: `dbt_test`
- Only runs if task 2 finished with `success`
- Command: `cd /opt/app/dbt_project && dbt test --profiles-dir . --project-dir .`
- Runs all tests defined in `schema.yml`

#### Task 4: `dbt_docs_generate`
- Only runs if task 3 finished with `success`
- Command: `cd /opt/app/dbt_project && dbt docs generate --profiles-dir . --project-dir .`
- Regenerates the lineage graph artifacts

### Moment T4 — DAG finishes

The containers **keep running**. Nothing stops. The scheduler returns to its loop, waiting for the next trigger.

---

## Summary: what your mental model needs to adjust

| Your original description | Reality |
|---|---|
| "Something triggers the need to start a container" | Containers are already always running |
| "Builds an Ubuntu environment with dbt, duckdb" | The `apache/airflow:2.9.1` image comes ready; `_PIP_ADDITIONAL_REQUIREMENTS` installs `dbt-duckdb` at container startup |
| "Uploads the entire project into the container" | The project is always accessible via volume; nothing is copied |
| "Airflow signals something to shut down the container" | Containers never stop on their own; they only stop when you tell them to |

---

## So what changes when the pipeline runs?

Only data. The project files (Python, SQL) do not change — they are only read and executed. What changes is:

1. **`data/warehouse.duckdb`** — receives new data
2. **Airflow's postgres** — receives execution records (this task ran, succeeded, took X seconds)
3. **Logs in `/opt/airflow/logs/`** — what each task printed

Everything else stays the same.

---

## Temporal diagram

```
TIME →

[you] docker compose up
           │
           ▼
  ┌─────────────────────────────────────────────────────────┐
  │ postgres (always running)                               │
  │ airflow-scheduler (always running, in a loop)           │
  │ airflow-webserver (always running)                      │
  └─────────────────────────────────────────────────────────┘
           │                    │                    │
           ▼                    ▼                    ▼
       [trigger 1]          [trigger 2]          [trigger 3]
           │                    │                    │
       runs DAG             runs DAG             runs DAG
       updates data         updates data         updates data
           │                    │                    │
       containers           containers           containers
       keep running         keep running         keep running
```

Each pipeline execution is just **one pulse of activity** inside containers that are always alive.
