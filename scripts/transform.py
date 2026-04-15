"""
transform.py  —  Silver layer
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Reads from raw.* (Bronze), cleans/validates data, and writes to staging.*
(Silver).  All transformations are idempotent via UPSERT.

Entry point for the Airflow DAG:  run_transform()
"""

import logging

import numpy as np
import pandas as pd

from scripts.db import log_run, read_table, upsert_dataframe

log = logging.getLogger(__name__)

VALID_STATUSES = {"delivered", "shipped", "processing", "canceled", "invoiced", "unavailable"}


# ──────────────────────────────────────────────
#  Individual entity transformations
# ──────────────────────────────────────────────

def transform_customers(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop_duplicates(subset=["customer_id"])
    df = df.dropna(subset=["customer_id", "customer_unique_id"])
    df["city"] = df["city"].str.strip().str.title()
    df["state"] = df["state"].str.strip().str.upper()
    df["state"] = df["state"].where(df["state"].str.len() == 2, other=None)
    return df[["customer_id", "customer_unique_id", "zip_code", "city", "state"]].copy()


def transform_sellers(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop_duplicates(subset=["seller_id"])
    df = df.dropna(subset=["seller_id"])
    df["city"] = df["city"].str.strip().str.title()
    df["state"] = df["state"].str.strip().str.upper()
    df["state"] = df["state"].where(df["state"].str.len() == 2, other=None)
    return df[["seller_id", "zip_code", "city", "state"]].copy()


def transform_products(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop_duplicates(subset=["product_id"])
    df = df.dropna(subset=["product_id"])
    df["category"] = (
        df["category"]
        .replace("", None)
        .str.strip()
        .str.lower()
        .fillna("unknown")
    )

    for col in ["weight_g", "length_cm", "height_cm", "width_cm"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
        # Negative measurements are impossible – nullify them
        df[col] = df[col].where(df[col] > 0, other=np.nan)

    return df[["product_id", "category", "weight_g", "length_cm", "height_cm", "width_cm"]].copy()


def transform_orders(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop_duplicates(subset=["order_id"])
    df = df.dropna(subset=["order_id", "customer_id"])

    ts_cols = [
        "purchase_timestamp", "approved_at",
        "delivered_carrier_date", "delivered_customer_date",
        "estimated_delivery_date",
    ]
    for col in ts_cols:
        df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)

    # Remove orders where delivery precedes purchase (impossible)
    mask_invalid = (
        df["delivered_customer_date"].notna()
        & (df["delivered_customer_date"] < df["purchase_timestamp"])
    )
    n_invalid = mask_invalid.sum()
    if n_invalid:
        log.warning("Dropping %d orders with delivery before purchase.", n_invalid)
    df = df[~mask_invalid]

    # Normalise status
    df["status"] = df["status"].str.strip().str.lower()
    df["status"] = df["status"].where(df["status"].isin(VALID_STATUSES), other="unknown")

    # Derived metrics
    df["delivery_days"] = (
        (df["delivered_customer_date"] - df["purchase_timestamp"])
        .dt.total_seconds()
        .div(86_400)
        .round(1)
    )
    df["is_late"] = (
        df["delivered_customer_date"].notna()
        & df["estimated_delivery_date"].notna()
        & (df["delivered_customer_date"] > df["estimated_delivery_date"])
    )

    return df[[
        "order_id", "customer_id", "status",
        "purchase_timestamp", "approved_at",
        "delivered_carrier_date", "delivered_customer_date",
        "estimated_delivery_date", "delivery_days", "is_late",
    ]].copy()


def transform_order_items(df: pd.DataFrame) -> pd.DataFrame:
    df = df.dropna(subset=["order_id", "order_item_id", "product_id", "seller_id"])
    df["order_item_id"] = pd.to_numeric(df["order_item_id"], errors="coerce")
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["freight_value"] = pd.to_numeric(df["freight_value"], errors="coerce")
    df["shipping_limit"] = pd.to_datetime(df["shipping_limit"], errors="coerce", utc=True)

    # Business rule: price must be positive, freight non-negative
    df = df[df["price"] > 0]
    df = df[df["freight_value"] >= 0]
    df = df.dropna(subset=["order_item_id", "price", "freight_value"])

    # Deduplicate: raw table may have duplicate rows from multiple pipeline runs
    df = df.drop_duplicates(subset=["order_id", "order_item_id"])

    return df[["order_id", "order_item_id", "product_id", "seller_id",
               "shipping_limit", "price", "freight_value"]].copy()


def transform_order_payments(df: pd.DataFrame) -> pd.DataFrame:
    df = df.dropna(subset=["order_id", "payment_sequential"])
    df["payment_sequential"] = pd.to_numeric(df["payment_sequential"], errors="coerce")
    df["payment_value"] = pd.to_numeric(df["payment_value"], errors="coerce")
    df["payment_installments"] = pd.to_numeric(df["payment_installments"], errors="coerce")
    df = df[df["payment_value"] > 0]
    df = df.dropna(subset=["payment_sequential", "payment_value"])

    valid_types = {"credit_card", "debit_card", "voucher", "bank_transfer", "not_defined"}
    df["payment_type"] = df["payment_type"].str.strip().str.lower()
    df["payment_type"] = df["payment_type"].where(df["payment_type"].isin(valid_types), other="not_defined")

    # Deduplicate: raw table may have duplicate rows from multiple pipeline runs
    df = df.drop_duplicates(subset=["order_id", "payment_sequential"])

    return df[["order_id", "payment_sequential", "payment_type",
               "payment_installments", "payment_value"]].copy()


def transform_order_reviews(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop_duplicates(subset=["review_id"])
    df = df.dropna(subset=["review_id", "order_id"])
    df["score"] = pd.to_numeric(df["score"], errors="coerce")
    # Clamp to valid range
    df["score"] = df["score"].clip(lower=1, upper=5)
    df["score"] = df["score"].round(0)
    df["creation_date"] = pd.to_datetime(df["creation_date"], errors="coerce", utc=True)
    df["answer_date"] = pd.to_datetime(df["answer_date"], errors="coerce", utc=True)

    for col in ["comment_title", "comment_message"]:
        df[col] = df[col].str.strip().replace("", None)

    return df[["review_id", "order_id", "score", "comment_title",
               "comment_message", "creation_date", "answer_date"]].copy()


# ──────────────────────────────────────────────
#  Orchestrate all Silver transformations
# ──────────────────────────────────────────────

PIPELINE: list[dict] = [
    {
        "entity": "customers",
        "fn": transform_customers,
        "conflict_cols": ["customer_id"],
    },
    {
        "entity": "sellers",
        "fn": transform_sellers,
        "conflict_cols": ["seller_id"],
    },
    {
        "entity": "products",
        "fn": transform_products,
        "conflict_cols": ["product_id"],
    },
    {
        "entity": "orders",
        "fn": transform_orders,
        "conflict_cols": ["order_id"],
    },
    {
        "entity": "order_items",
        "fn": transform_order_items,
        "conflict_cols": ["order_id", "order_item_id"],
    },
    {
        "entity": "order_payments",
        "fn": transform_order_payments,
        "conflict_cols": ["order_id", "payment_sequential"],
    },
    {
        "entity": "order_reviews",
        "fn": transform_order_reviews,
        "conflict_cols": ["review_id"],
    },
]


def run_transform(**kwargs) -> None:
    """Airflow-callable entry point for the transform task."""
    log.info("=== TRANSFORM (Silver layer) ===")
    total = 0
    errors = 0

    for step in PIPELINE:
        entity = step["entity"]
        try:
            raw_df = read_table("raw", entity)
            log.info("  %s: read %d raw rows", entity, len(raw_df))

            clean_df = step["fn"](raw_df)
            log.info("  %s: %d rows after cleaning", entity, len(clean_df))

            n = upsert_dataframe(clean_df, "staging", entity, step["conflict_cols"])
            log.info("  %s: upserted %d rows → staging.%s", entity, n, entity)
            total += len(clean_df)
        except Exception as exc:
            log.error("Transform failed for %s: %s", entity, exc, exc_info=True)
            log_run("ecommerce_etl", "transform_to_silver", "error", error=str(exc))
            errors += 1

    if errors:
        raise RuntimeError(f"Transform finished with {errors} error(s). Check logs.")

    log_run("ecommerce_etl", "transform_to_silver", "success", rows=total)
    log.info("Transform complete. Total rows processed: %d", total)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    run_transform()
