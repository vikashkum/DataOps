"""
conftest.py — shared pytest fixtures
"""

import pandas as pd
import pytest


# ── Customers ──────────────────────────────────────────────────────────

@pytest.fixture
def raw_customers():
    return pd.DataFrame(
        {
            "customer_id":        ["c1", "c2", "c2", "c3", None],
            "customer_unique_id": ["u1", "u2", "u2", "u3", "u4"],
            "zip_code":           ["10001", "90210", "90210", "30301", "77001"],
            "city":               ["new york", "los angeles", "los angeles", "atlanta", "houston"],
            "state":              ["NY", "CA", "CA", "GA", "TX"],
        }
    )


# ── Orders ─────────────────────────────────────────────────────────────

@pytest.fixture
def raw_orders():
    return pd.DataFrame(
        {
            "order_id":               ["o1", "o2", "o3", "o1"],
            "customer_id":            ["c1", "c2", "c3", "c1"],
            "status":                 ["delivered", "shipped", "CANCELED", "delivered"],
            "purchase_timestamp": [
                "2023-01-10T10:00:00", "2023-03-15T12:00:00",
                "2023-06-01T09:00:00", "2023-01-10T10:00:00",
            ],
            "approved_at": [
                "2023-01-10T11:00:00", "2023-03-15T13:00:00",
                "2023-06-01T10:00:00", "2023-01-10T11:00:00",
            ],
            "delivered_carrier_date": [
                "2023-01-12T08:00:00", "2023-03-17T08:00:00",
                None, "2023-01-12T08:00:00",
            ],
            # o2: delivered BEFORE purchase → should be dropped
            "delivered_customer_date": [
                "2023-01-15T14:00:00", "2023-03-10T10:00:00",
                None, "2023-01-15T14:00:00",
            ],
            "estimated_delivery_date": [
                "2023-01-20T00:00:00", "2023-03-25T00:00:00",
                "2023-06-20T00:00:00", "2023-01-20T00:00:00",
            ],
        }
    )


# ── Order items ────────────────────────────────────────────────────────

@pytest.fixture
def raw_order_items():
    return pd.DataFrame(
        {
            "order_id":      ["o1", "o1", "o2", "o3"],
            "order_item_id": ["1",  "2",  "1",  "1"],
            "product_id":    ["p1", "p2", "p1", "p3"],
            "seller_id":     ["s1", "s1", "s2", "s2"],
            "shipping_limit": ["2023-01-12T00:00:00"] * 4,
            "price":         ["99.99", "-5.00", "200.00", "50.00"],
            "freight_value": ["10.00", "8.00",  "15.00",  "0.00"],
        }
    )


# ── Products ───────────────────────────────────────────────────────────

@pytest.fixture
def raw_products():
    return pd.DataFrame(
        {
            "product_id": ["p1", "p2", "p3", "p4"],
            "category":   ["Electronics", None, "BOOKS", ""],
            "weight_g":   ["500", "-100", "250", "abc"],
            "length_cm":  ["20", "30", "15", "10"],
            "height_cm":  ["5",  "10", "8",  "3"],
            "width_cm":   ["15", "20", "12", "7"],
        }
    )
