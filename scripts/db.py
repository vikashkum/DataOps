"""Database connection and helper utilities."""

import logging
import os
from contextlib import contextmanager

import pandas as pd
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()

log = logging.getLogger(__name__)

DB_CONFIG = {
    "host": os.getenv("DATAOPS_DB_HOST", "localhost"),
    "port": int(os.getenv("DATAOPS_DB_PORT", 5432)),
    "dbname": os.getenv("DATAOPS_DB_NAME", "dataops"),
    "user": os.getenv("DATAOPS_DB_USER", "dataops"),
    "password": os.getenv("DATAOPS_DB_PASSWORD", "dataops"),
}


@contextmanager
def get_connection():
    """Yield a psycopg2 connection, committing on success and rolling back on error."""
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def execute_sql(sql: str, params=None) -> None:
    """Execute a single SQL statement."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)


def read_table(schema: str, table: str, where: str = "") -> pd.DataFrame:
    """Read an entire table (or filtered subset) into a DataFrame."""
    query = f'SELECT * FROM {schema}."{table}"'
    if where:
        query += f" WHERE {where}"
    with get_connection() as conn:
        return pd.read_sql(query, conn)


def _to_python(val):
    """Convert pandas NA sentinels and numpy scalar types to Python native types."""
    try:
        if pd.isna(val):
            return None
    except (TypeError, ValueError):
        pass
    # numpy scalars (int64, float64, int16, etc.) expose .item() → Python native
    if hasattr(val, "item"):
        return val.item()
    return val


def bulk_insert(df: pd.DataFrame, schema: str, table: str) -> int:
    """
    Fast bulk-insert a DataFrame using psycopg2 execute_values.
    Skips rows that would violate ON CONFLICT constraints.
    Returns the number of rows inserted.
    """
    if df.empty:
        return 0

    cols = list(df.columns)
    col_list = ", ".join(f'"{c}"' for c in cols)
    values = [
        tuple(_to_python(v) for v in row)
        for row in df.itertuples(index=False, name=None)
    ]

    sql = (
        f"INSERT INTO {schema}.\"{table}\" ({col_list}) VALUES %s "
        "ON CONFLICT DO NOTHING"
    )

    with get_connection() as conn:
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, sql, values, page_size=1000)
            return cur.rowcount


def upsert_dataframe(
    df: pd.DataFrame, schema: str, table: str, conflict_cols: list
) -> int:
    """
    Upsert a DataFrame: INSERT ... ON CONFLICT (...) DO UPDATE SET ...
    Returns the number of rows affected.
    """
    if df.empty:
        return 0

    cols = list(df.columns)
    col_list = ", ".join(f'"{c}"' for c in cols)
    update_cols = [c for c in cols if c not in conflict_cols]
    update_set = ", ".join(f'"{c}" = EXCLUDED."{c}"' for c in update_cols)
    conflict_target = ", ".join(f'"{c}"' for c in conflict_cols)
    values = [
        tuple(_to_python(v) for v in row)
        for row in df.itertuples(index=False, name=None)
    ]

    if update_set:
        sql = (
            f'INSERT INTO {schema}."{table}" ({col_list}) VALUES %s '
            f"ON CONFLICT ({conflict_target}) DO UPDATE SET {update_set}"
        )
    else:
        sql = (
            f'INSERT INTO {schema}."{table}" ({col_list}) VALUES %s '
            f"ON CONFLICT ({conflict_target}) DO NOTHING"
        )

    with get_connection() as conn:
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, sql, values, page_size=1000)
            return cur.rowcount


def get_watermark(entity: str) -> str:
    """Return the last_loaded_at watermark for an entity."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT last_loaded_at FROM meta.watermarks WHERE entity = %s",
                (entity,),
            )
            row = cur.fetchone()
            return str(row[0]) if row else "1970-01-01"


def set_watermark(entity: str, ts: str) -> None:
    """Update the watermark for an entity."""
    execute_sql(
        """
        INSERT INTO meta.watermarks (entity, last_loaded_at)
        VALUES (%s, %s)
        ON CONFLICT (entity) DO UPDATE SET last_loaded_at = EXCLUDED.last_loaded_at
        """,
        (entity, ts),
    )


def log_run(dag_id: str, task_id: str, status: str, rows: int = 0, error: str = None) -> None:
    """Record a pipeline run in meta.pipeline_runs."""
    execute_sql(
        """
        INSERT INTO meta.pipeline_runs (dag_id, task_id, status, rows_processed, finished_at, error_message)
        VALUES (%s, %s, %s, %s, now(), %s)
        """,
        (dag_id, task_id, status, rows, error),
    )
