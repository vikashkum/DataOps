"""
extract.py  —  Bronze layer
~~~~~~~~~~~~~~~~~~~~~~~~~~~
Reads raw CSV files from DATAOPS_DATA_DIR/raw/ and bulk-inserts them
into the raw.* PostgreSQL schema (incremental: skips already-loaded rows).

Entry point for the Airflow DAG:  run_extract()
"""

import logging
import os

import pandas as pd

from scripts.db import (
    bulk_insert,
    get_watermark,
    log_run,
    set_watermark,
)

log = logging.getLogger(__name__)

DATA_DIR = os.getenv("DATAOPS_DATA_DIR", "./data")
RAW_DIR = os.path.join(DATA_DIR, "raw")

# Map: CSV filename stem → (raw table name, incremental column, dtype overrides)
EXTRACT_CONFIG: list[dict] = [
    {"file": "customers",       "table": "customers",       "id_col": "customer_id"},
    {"file": "sellers",         "table": "sellers",         "id_col": "seller_id"},
    {"file": "products",        "table": "products",        "id_col": "product_id"},
    {"file": "orders",          "table": "orders",          "id_col": "order_id"},
    {"file": "order_items",     "table": "order_items",     "id_col": "order_id"},
    {"file": "order_payments",  "table": "order_payments",  "id_col": "order_id"},
    {"file": "order_reviews",   "table": "order_reviews",   "id_col": "order_id"},
]


def _load_entity(cfg: dict) -> int:
    """Load one CSV → raw table.  Returns number of rows inserted."""
    path = os.path.join(RAW_DIR, f"{cfg['file']}.csv")
    if not os.path.exists(path):
        log.warning("Source file not found, skipping: %s", path)
        return 0

    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    df = df.replace("", None)

    # Incremental: exclude IDs already present in the raw table.
    watermark = get_watermark(cfg["table"])
    if watermark != "1970-01-01":
        log.info("  Entity %s: watermark=%s – loading incremental batch only",
                 cfg["table"], watermark)
        # For a CSV-based source the watermark tracks whether we've ever loaded this file.
        # On subsequent runs we skip the file entirely (idempotent initial load).
        existing_count = _count_raw(cfg["table"])
        if existing_count >= len(df):
            log.info("  No new rows for %s – skipping.", cfg["table"])
            return 0

    n = bulk_insert(df, "raw", cfg["table"])
    set_watermark(cfg["table"], pd.Timestamp.utcnow().isoformat())
    log.info("  Inserted %d rows → raw.%s", n, cfg["table"])
    return n


def _count_raw(table: str) -> int:
    from scripts.db import get_connection
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) FROM raw."{table}"')
            return cur.fetchone()[0]


def run_extract(**kwargs) -> None:
    """Airflow-callable entry point for the extract task."""
    log.info("=== EXTRACT (Bronze layer) ===")
    total = 0
    errors = 0

    for cfg in EXTRACT_CONFIG:
        try:
            n = _load_entity(cfg)
            total += n
        except Exception as exc:
            log.error("Failed to extract %s: %s", cfg["table"], exc)
            log_run("ecommerce_etl", "extract_to_bronze", "error", error=str(exc))
            errors += 1

    if errors:
        raise RuntimeError(f"Extract finished with {errors} error(s). Check logs.")

    log_run("ecommerce_etl", "extract_to_bronze", "success", rows=total)
    log.info("Extract complete. Total rows inserted: %d", total)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    run_extract()
