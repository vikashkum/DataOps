"""
etl_pipeline.py  —  Airflow DAG
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Orchestrates the nightly e-commerce ETL:

    extract_to_bronze  →  transform_to_silver  →  load_to_gold

Schedule:  daily at midnight UTC
Retries:   2 (5-minute delay)
"""

import sys
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# Make project scripts importable inside the container
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from scripts.extract import run_extract      # noqa: E402
from scripts.transform import run_transform  # noqa: E402
from scripts.load import run_load            # noqa: E402

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
    description="E-commerce ETL: CSV → Bronze → Silver → Gold",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ecommerce", "etl", "portfolio"],
    doc_md="""
## E-Commerce ETL Pipeline

| Layer   | Schema     | Description                        |
|---------|------------|------------------------------------|
| Bronze  | raw        | Exact copy of source CSV data      |
| Silver  | staging    | Cleaned, typed, de-duplicated      |
| Gold    | warehouse  | Star schema ready for BI queries   |

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

    # ── Task 1: Bronze layer ────────────────────────────────────────────
    extract = PythonOperator(
        task_id="extract_to_bronze",
        python_callable=run_extract,
        doc_md="Load raw CSVs → raw.* schema (incremental, idempotent).",
    )

    # ── Task 2: Silver layer ────────────────────────────────────────────
    transform = PythonOperator(
        task_id="transform_to_silver",
        python_callable=run_transform,
        doc_md="Clean + validate raw data → staging.* schema.",
    )

    # ── Task 3: Gold layer ──────────────────────────────────────────────
    load = PythonOperator(
        task_id="load_to_gold",
        python_callable=run_load,
        doc_md="Build star schema → warehouse.* (dim_* + fact_order_items).",
    )

    # ── DAG dependency chain ────────────────────────────────────────────
    check_source >> extract >> transform >> load
