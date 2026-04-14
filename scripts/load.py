"""
load.py  —  Gold layer
~~~~~~~~~~~~~~~~~~~~~~~~
Reads from staging.* (Silver), builds the star schema, and writes to
warehouse.*  All dimension loads use SCD Type 1 (upsert).
The fact table uses INSERT ... ON CONFLICT DO NOTHING for idempotency.

Entry point for the Airflow DAG:  run_load()
"""

import logging

import pandas as pd

from scripts.db import get_connection, log_run, read_table, upsert_dataframe

log = logging.getLogger(__name__)


# ──────────────────────────────────────────────
#  Dimension builders
# ──────────────────────────────────────────────

def build_dim_date(start: str = "2020-01-01", end: str = "2026-12-31") -> pd.DataFrame:
    """Generate a date dimension covering the full analytical window."""
    dates = pd.date_range(start=start, end=end, freq="D")
    return pd.DataFrame(
        {
            "date_key": dates.strftime("%Y%m%d").astype(int),
            "full_date": dates.date,
            "day": dates.day.astype("int16"),
            "month": dates.month.astype("int16"),
            "year": dates.year.astype("int16"),
            "quarter": dates.quarter.astype("int16"),
            "day_of_week": dates.dayofweek.astype("int16"),
            "day_name": dates.day_name(),
            "month_name": dates.month_name(),
            "is_weekend": dates.dayofweek >= 5,
        }
    )


def load_dim_date() -> int:
    df = build_dim_date()
    n = upsert_dataframe(df, "warehouse", "dim_date", ["date_key"])
    log.info("  dim_date: %d rows", n)
    return n


def load_dim_customer() -> int:
    df = read_table("staging", "customers")
    out = df[["customer_id", "customer_unique_id", "city", "state"]].copy()
    n = upsert_dataframe(out, "warehouse", "dim_customer", ["customer_id"])
    log.info("  dim_customer: %d rows", n)
    return n


def load_dim_product() -> int:
    df = read_table("staging", "products")
    out = df[["product_id", "category", "weight_g"]].copy()
    n = upsert_dataframe(out, "warehouse", "dim_product", ["product_id"])
    log.info("  dim_product: %d rows", n)
    return n


def load_dim_seller() -> int:
    df = read_table("staging", "sellers")
    out = df[["seller_id", "city", "state"]].copy()
    n = upsert_dataframe(out, "warehouse", "dim_seller", ["seller_id"])
    log.info("  dim_seller: %d rows", n)
    return n


# ──────────────────────────────────────────────
#  Fact builder
# ──────────────────────────────────────────────

def _fetch_dim_keys() -> tuple[dict, dict, dict]:
    """Return lookup dicts: id → surrogate key for customer, product, seller."""
    with get_connection() as conn:
        cust_map = pd.read_sql(
            "SELECT customer_id, customer_key FROM warehouse.dim_customer", conn
        ).set_index("customer_id")["customer_key"].to_dict()

        prod_map = pd.read_sql(
            "SELECT product_id, product_key FROM warehouse.dim_product", conn
        ).set_index("product_id")["product_key"].to_dict()

        sell_map = pd.read_sql(
            "SELECT seller_id, seller_key FROM warehouse.dim_seller", conn
        ).set_index("seller_id")["seller_key"].to_dict()

    return cust_map, prod_map, sell_map


def load_fact_order_items() -> int:
    """
    Join staging.order_items → orders → payments (aggregated) → reviews.
    Produce one row per order line item (the grain of fact_order_items).
    """
    items = read_table("staging", "order_items")
    orders = read_table("staging", "orders")[
        ["order_id", "customer_id", "status",
         "purchase_timestamp", "delivery_days", "is_late"]
    ]
    payments = (
        read_table("staging", "order_payments")
        .groupby("order_id", as_index=False)["payment_value"]
        .sum()
        .rename(columns={"payment_value": "total_payment"})
    )
    reviews = (
        read_table("staging", "order_reviews")
        .groupby("order_id", as_index=False)["score"]
        .mean()
        .rename(columns={"score": "review_score"})
    )

    # Join everything to items
    fact = items.merge(orders, on="order_id", how="left")
    fact = fact.merge(payments, on="order_id", how="left")
    fact = fact.merge(reviews, on="order_id", how="left")

    # Resolve dimension keys
    cust_map, prod_map, sell_map = _fetch_dim_keys()
    fact["customer_key"] = fact["customer_id"].map(cust_map)
    fact["product_key"] = fact["product_id"].map(prod_map)
    fact["seller_key"] = fact["seller_id"].map(sell_map)

    # Date key from purchase timestamp
    fact["purchase_timestamp"] = pd.to_datetime(fact["purchase_timestamp"], utc=True)
    fact["date_key"] = fact["purchase_timestamp"].dt.strftime("%Y%m%d").astype("Int64")
    fact["purchase_year"] = fact["purchase_timestamp"].dt.year.astype("Int16")
    fact["purchase_month"] = fact["purchase_timestamp"].dt.month.astype("Int16")

    # Round review_score to nearest integer
    fact["review_score"] = fact["review_score"].round(0)

    out = fact[[
        "order_id", "order_item_id",
        "customer_key", "product_key", "seller_key", "date_key",
        "status", "price", "freight_value", "total_payment",
        "review_score", "delivery_days", "is_late",
        "purchase_year", "purchase_month",
    ]].rename(columns={"status": "order_status"})

    n = upsert_dataframe(out, "warehouse", "fact_order_items", ["order_id", "order_item_id"])
    log.info("  fact_order_items: %d rows", n)
    return n


# ──────────────────────────────────────────────
#  Orchestrate Gold load
# ──────────────────────────────────────────────

def run_load(**kwargs) -> None:
    """Airflow-callable entry point for the load (Gold) task."""
    log.info("=== LOAD (Gold layer) ===")
    total = 0
    errors = 0

    steps = [
        ("dim_date",          load_dim_date),
        ("dim_customer",      load_dim_customer),
        ("dim_product",       load_dim_product),
        ("dim_seller",        load_dim_seller),
        ("fact_order_items",  load_fact_order_items),
    ]

    for name, fn in steps:
        try:
            n = fn()
            total += n
        except Exception as exc:
            log.error("Load failed for %s: %s", name, exc, exc_info=True)
            log_run("ecommerce_etl", "load_to_gold", "error", error=str(exc))
            errors += 1

    if errors:
        raise RuntimeError(f"Load finished with {errors} error(s). Check logs.")

    log_run("ecommerce_etl", "load_to_gold", "success", rows=total)
    log.info("Gold load complete. Total rows: %d", total)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    run_load()
