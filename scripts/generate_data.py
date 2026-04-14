"""
generate_data.py
~~~~~~~~~~~~~~~~
Generate synthetic e-commerce CSVs that mimic the Olist public dataset.

Usage:  python scripts/generate_data.py
Output: data/raw/{customers,sellers,products,orders,
                  order_items,order_payments,order_reviews}.csv
"""

import hashlib
import logging
import os
import random
import uuid
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

SEED = 42
rng = np.random.default_rng(SEED)
random.seed(SEED)

DATA_DIR = os.getenv("DATAOPS_DATA_DIR", "./data")
RAW_DIR = os.path.join(DATA_DIR, "raw")

# ──────────────────────────────────────────────
#  Reference data
# ──────────────────────────────────────────────
US_LOCATIONS: list[tuple[str, str]] = [
    ("Los Angeles", "CA"), ("San Francisco", "CA"), ("San Diego", "CA"),
    ("Houston", "TX"), ("Dallas", "TX"), ("Austin", "TX"),
    ("Miami", "FL"), ("Orlando", "FL"), ("Tampa", "FL"),
    ("New York", "NY"), ("Buffalo", "NY"), ("Rochester", "NY"),
    ("Chicago", "IL"), ("Aurora", "IL"), ("Naperville", "IL"),
    ("Phoenix", "AZ"), ("Tucson", "AZ"), ("Mesa", "AZ"),
    ("Seattle", "WA"), ("Spokane", "WA"), ("Tacoma", "WA"),
    ("Denver", "CO"), ("Colorado Springs", "CO"), ("Aurora", "CO"),
    ("Portland", "OR"), ("Salem", "OR"), ("Eugene", "OR"),
    ("Nashville", "TN"), ("Memphis", "TN"), ("Knoxville", "TN"),
    ("Charlotte", "NC"), ("Raleigh", "NC"), ("Greensboro", "NC"),
    ("Columbus", "OH"), ("Cleveland", "OH"), ("Cincinnati", "OH"),
    ("Detroit", "MI"), ("Grand Rapids", "MI"), ("Warren", "MI"),
    ("Philadelphia", "PA"), ("Pittsburgh", "PA"), ("Allentown", "PA"),
]

PRODUCT_CATEGORIES: list[str] = [
    "electronics", "computers_accessories", "mobile_phones",
    "books_general", "clothing_fashion", "toys_games",
    "sports_leisure", "home_garden", "health_beauty",
    "auto_parts", "food_drink", "office_supplies",
    "music_instruments", "movies_dvd", "industrial_tools",
]
CATEGORY_WEIGHTS = np.array(
    [0.15, 0.12, 0.10, 0.09, 0.09, 0.08,
     0.07, 0.07, 0.06, 0.05, 0.05, 0.04,
     0.02, 0.01, 0.01]
)
CATEGORY_WEIGHTS /= CATEGORY_WEIGHTS.sum()

PAYMENT_TYPES = ["credit_card", "debit_card", "voucher", "bank_transfer"]
PAYMENT_WEIGHTS = [0.70, 0.15, 0.10, 0.05]

ORDER_STATUSES = ["delivered", "shipped", "processing", "canceled", "invoiced"]
STATUS_WEIGHTS = [0.80, 0.08, 0.05, 0.04, 0.03]

N_CUSTOMERS = 10_000
N_SELLERS = 500
N_PRODUCTS = 1_200
N_ORDERS = 50_000


# ──────────────────────────────────────────────
#  Helpers
# ──────────────────────────────────────────────
def new_id() -> str:
    return uuid.uuid4().hex


def short_hash(s: str) -> str:
    return hashlib.md5(s.encode()).hexdigest()


def rand_zip() -> str:
    return f"{rng.integers(10000, 99999):05d}"


def rand_timestamp(start: datetime, end: datetime) -> datetime:
    delta = (end - start).total_seconds()
    return start + timedelta(seconds=float(rng.uniform(0, delta)))


# ──────────────────────────────────────────────
#  Generators
# ──────────────────────────────────────────────
def gen_customers(n: int) -> pd.DataFrame:
    log.info("Generating %d customers …", n)
    ids = [new_id() for _ in range(n)]
    locs = [random.choice(US_LOCATIONS) for _ in range(n)]
    return pd.DataFrame(
        {
            "customer_id": ids,
            "customer_unique_id": [short_hash(cid) for cid in ids],
            "zip_code": [rand_zip() for _ in range(n)],
            "city": [loc[0] for loc in locs],
            "state": [loc[1] for loc in locs],
        }
    )


def gen_sellers(n: int) -> pd.DataFrame:
    log.info("Generating %d sellers …", n)
    locs = [random.choice(US_LOCATIONS) for _ in range(n)]
    return pd.DataFrame(
        {
            "seller_id": [new_id() for _ in range(n)],
            "zip_code": [rand_zip() for _ in range(n)],
            "city": [loc[0] for loc in locs],
            "state": [loc[1] for loc in locs],
        }
    )


def gen_products(n: int) -> pd.DataFrame:
    log.info("Generating %d products …", n)
    categories = rng.choice(PRODUCT_CATEGORIES, size=n, p=CATEGORY_WEIGHTS)
    # ~3% missing category to exercise null handling
    mask = rng.random(n) < 0.03
    categories = np.where(mask, None, categories)
    return pd.DataFrame(
        {
            "product_id": [new_id() for _ in range(n)],
            "category": categories,
            "weight_g": rng.integers(50, 20_000, size=n).astype(float),
            "length_cm": rng.integers(10, 100, size=n).astype(float),
            "height_cm": rng.integers(5, 50, size=n).astype(float),
            "width_cm": rng.integers(5, 80, size=n).astype(float),
        }
    )


def gen_orders(
    n: int, customer_ids: list[str]
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Return (orders, order_items, order_payments, order_reviews)."""
    log.info("Generating %d orders …", n)

    start = datetime(2022, 1, 1)
    end = datetime(2024, 12, 31)

    order_ids = [new_id() for _ in range(n)]
    cust_sample = rng.choice(customer_ids, size=n)
    statuses = rng.choice(ORDER_STATUSES, size=n, p=STATUS_WEIGHTS)
    purchase_times = [rand_timestamp(start, end) for _ in range(n)]
    purchase_times.sort()  # chronological order

    approved_delta = rng.uniform(0.5, 48, size=n)  # hours
    carrier_delta = rng.uniform(1, 10, size=n)  # days after approved
    customer_delta = rng.uniform(1, 7, size=n)  # days after carrier
    estimated_delta = rng.uniform(5, 20, size=n)  # days after purchase

    approved_at, carrier_date, customer_date, estimated_date = [], [], [], []
    for i, pt in enumerate(purchase_times):
        approved_at.append(pt + timedelta(hours=float(approved_delta[i])))
        carrier_date.append(approved_at[-1] + timedelta(days=float(carrier_delta[i])))
        customer_date.append(carrier_date[-1] + timedelta(days=float(customer_delta[i])))
        estimated_date.append(pt + timedelta(days=float(estimated_delta[i])))

    orders_df = pd.DataFrame(
        {
            "order_id": order_ids,
            "customer_id": cust_sample,
            "status": statuses,
            "purchase_timestamp": [t.isoformat() for t in purchase_times],
            "approved_at": [t.isoformat() for t in approved_at],
            "delivered_carrier_date": [t.isoformat() for t in carrier_date],
            "delivered_customer_date": [
                t.isoformat() if statuses[i] == "delivered" else None
                for i, t in enumerate(customer_date)
            ],
            "estimated_delivery_date": [t.isoformat() for t in estimated_date],
        }
    )

    # ── Order items ──
    log.info("Generating order items …")
    items_rows = []
    for oid in order_ids:
        n_items = int(rng.choice([1, 2, 3, 4], p=[0.60, 0.25, 0.10, 0.05]))
        for item_num in range(1, n_items + 1):
            items_rows.append(
                {
                    "order_id": oid,
                    "order_item_id": item_num,
                    "product_id": None,  # filled below
                    "seller_id": None,
                    "shipping_limit": None,
                    "price": round(float(rng.uniform(5, 2000)), 2),
                    "freight_value": round(float(rng.uniform(2, 80)), 2),
                }
            )
    items_df = pd.DataFrame(items_rows)

    # Resolve FK references (done after generation for speed)
    product_ids_list = PRODUCT_IDS_CACHE  # set by caller
    seller_ids_list = SELLER_IDS_CACHE

    items_df["product_id"] = rng.choice(product_ids_list, size=len(items_df))
    items_df["seller_id"] = rng.choice(seller_ids_list, size=len(items_df))

    # shipping_limit = purchase_time + random days
    purchase_map = dict(zip(order_ids, purchase_times))
    items_df["shipping_limit"] = items_df["order_id"].map(
        lambda oid: (purchase_map[oid] + timedelta(days=float(rng.uniform(3, 14)))).isoformat()
    )

    # ── Order payments ──
    log.info("Generating order payments …")
    payments_rows = []
    for oid in order_ids:
        n_payments = int(rng.choice([1, 2], p=[0.90, 0.10]))
        order_total = float(
            items_df[items_df["order_id"] == oid][["price", "freight_value"]].sum().sum()
        )
        remaining = order_total
        for seq in range(1, n_payments + 1):
            ptype = rng.choice(PAYMENT_TYPES, p=PAYMENT_WEIGHTS)
            installments = int(rng.choice([1, 2, 3, 6, 12], p=[0.55, 0.15, 0.12, 0.10, 0.08]))
            amount = remaining if seq == n_payments else round(remaining * float(rng.uniform(0.3, 0.7)), 2)
            remaining = round(remaining - amount, 2)
            payments_rows.append(
                {
                    "order_id": oid,
                    "payment_sequential": seq,
                    "payment_type": ptype,
                    "payment_installments": installments,
                    "payment_value": max(amount, 0.01),
                }
            )
    payments_df = pd.DataFrame(payments_rows)

    # ── Order reviews ──
    log.info("Generating order reviews …")
    # ~88% of delivered orders get a review
    delivered_orders = [oid for oid, s in zip(order_ids, statuses) if s == "delivered"]
    review_orders = rng.choice(
        delivered_orders,
        size=int(len(delivered_orders) * 0.88),
        replace=False,
    )
    scores = rng.choice([1, 2, 3, 4, 5], size=len(review_orders), p=[0.05, 0.07, 0.13, 0.25, 0.50])
    reviews_df = pd.DataFrame(
        {
            "review_id": [new_id() for _ in range(len(review_orders))],
            "order_id": review_orders,
            "score": scores,
            "comment_title": [None] * len(review_orders),
            "comment_message": [None] * len(review_orders),
            "creation_date": [
                (purchase_map[oid] + timedelta(days=float(rng.uniform(10, 30)))).isoformat()
                for oid in review_orders
            ],
            "answer_date": [None] * len(review_orders),
        }
    )

    return orders_df, items_df, payments_df, reviews_df


# ──────────────────────────────────────────────
#  Caches to avoid re-creating IDs inside loops
# ──────────────────────────────────────────────
PRODUCT_IDS_CACHE: list[str] = []
SELLER_IDS_CACHE: list[str] = []


def generate_all() -> None:
    global PRODUCT_IDS_CACHE, SELLER_IDS_CACHE

    os.makedirs(RAW_DIR, exist_ok=True)

    customers_df = gen_customers(N_CUSTOMERS)
    sellers_df = gen_sellers(N_SELLERS)
    products_df = gen_products(N_PRODUCTS)

    PRODUCT_IDS_CACHE = products_df["product_id"].tolist()
    SELLER_IDS_CACHE = sellers_df["seller_id"].tolist()

    orders_df, items_df, payments_df, reviews_df = gen_orders(
        N_ORDERS, customers_df["customer_id"].tolist()
    )

    files = {
        "customers": customers_df,
        "sellers": sellers_df,
        "products": products_df,
        "orders": orders_df,
        "order_items": items_df,
        "order_payments": payments_df,
        "order_reviews": reviews_df,
    }

    for name, df in files.items():
        path = os.path.join(RAW_DIR, f"{name}.csv")
        df.to_csv(path, index=False)
        log.info("  Wrote %s → %d rows", path, len(df))

    log.info("Data generation complete.")


if __name__ == "__main__":
    generate_all()
