"""
etl_pipeline.py  —  Airflow DAG  (Enhancement 01: dbt Gold layer)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Pipeline:

    check_source_files
          ↓
    extract_to_bronze      CSV → raw.*
          ↓
    transform_to_silver    raw.* → staging.*  (Python / Pandas)
          ↓
    dbt_run_gold           staging.* → warehouse.*  (dbt models)
          ↓
    dbt_test_gold          run dbt tests on warehouse.*

Schedule:  daily at midnight UTC
Retries:   2 (5-minute delay)
"""

import sys
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from scripts.extract import run_extract      # noqa: E402
from scripts.transform import run_transform  # noqa: E402

DBT_DIR = "/opt/airflow/dbt"
DBT_CMD = f"cd {DBT_DIR} && dbt"
DBT_FLAGS = f"--profiles-dir {DBT_DIR} --target prod"

default_args = {
    "owner": "dataops",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="ecommerce_etl",
    default_args=default_args,
    description="E-commerce ETL: CSV → Bronze → Silver → Gold (dbt)",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ecommerce", "etl", "dbt", "portfolio"],
    doc_md="""
## E-Commerce ETL Pipeline

| Layer  | Tool        | Schema     | Description                      |
|--------|-------------|------------|----------------------------------|
| Bronze | Python      | raw        | Exact copy of source CSV data    |
| Silver | Python      | staging    | Cleaned, typed, de-duplicated    |
| Gold   | **dbt**     | warehouse  | Star schema — dim_* + fact_*     |

**dbt docs:** `dbt docs serve` inside the container
**Dashboard:** http://localhost:3000  (Metabase)
""",
) as dag:

    # ── Task 0: verify source data exists ───────────────────────────────
    check_source = BashOperator(
        task_id="check_source_files",
        bash_command=(
            "ls ${DATAOPS_DATA_DIR}/raw/orders.csv "
            "${DATAOPS_DATA_DIR}/raw/customers.csv || "
            "(echo 'Source files missing – run scripts/generate_data.py first' && exit 1)"
        ),
    )

    # ── Task 1: Bronze layer ─────────────────────────────────────────────
    extract = PythonOperator(
        task_id="extract_to_bronze",
        python_callable=run_extract,
        doc_md="Load raw CSVs → raw.* schema (incremental, idempotent).",
    )

    # ── Task 2: Silver layer ─────────────────────────────────────────────
    transform = PythonOperator(
        task_id="transform_to_silver",
        python_callable=run_transform,
        doc_md="Clean + validate raw data → staging.* schema.",
    )

    # ── Task 3: Gold layer via dbt ───────────────────────────────────────
    dbt_run = BashOperator(
        task_id="dbt_run_gold",
        bash_command=f"{DBT_CMD} run {DBT_FLAGS}",
        doc_md="Build warehouse.* star schema using dbt models.",
    )

    # ── Task 4: dbt data quality tests ───────────────────────────────────
    dbt_test = BashOperator(
        task_id="dbt_test_gold",
        bash_command=f"{DBT_CMD} test {DBT_FLAGS}",
        doc_md="Run dbt tests: not_null, unique, accepted_values, relationships.",
    )

    # ── DAG dependency chain ─────────────────────────────────────────────
    check_source >> extract >> transform >> dbt_run >> dbt_test
