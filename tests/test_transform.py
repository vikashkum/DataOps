"""
test_transform.py
~~~~~~~~~~~~~~~~~
Unit tests for scripts/transform.py Silver-layer transformations.
No database connection required — all transformations are pure-function pandas.
"""

import math

import pandas as pd

from scripts.transform import (
    transform_customers,
    transform_order_items,
    transform_orders,
    transform_products,
)


# ──────────────────────────────────────────────
#  Customers
# ──────────────────────────────────────────────

class TestTransformCustomers:
    def test_deduplication(self, raw_customers):
        out = transform_customers(raw_customers)
        assert out["customer_id"].nunique() == len(out), "Duplicate customer_ids remain"

    def test_drops_null_customer_id(self, raw_customers):
        out = transform_customers(raw_customers)
        assert out["customer_id"].isna().sum() == 0

    def test_city_title_case(self, raw_customers):
        out = transform_customers(raw_customers)
        assert out.loc[out["customer_id"] == "c1", "city"].iloc[0] == "New York"

    def test_state_upper(self, raw_customers):
        out = transform_customers(raw_customers)
        for s in out["state"].dropna():
            assert s == s.upper()

    def test_output_columns(self, raw_customers):
        out = transform_customers(raw_customers)
        expected = {"customer_id", "customer_unique_id", "zip_code", "city", "state"}
        assert set(out.columns) == expected


# ──────────────────────────────────────────────
#  Orders
# ──────────────────────────────────────────────

class TestTransformOrders:
    def test_deduplication(self, raw_orders):
        out = transform_orders(raw_orders)
        assert out["order_id"].nunique() == len(out)

    def test_drops_delivery_before_purchase(self, raw_orders):
        # o2 has delivered_customer_date < purchase_timestamp
        out = transform_orders(raw_orders)
        assert "o2" not in out["order_id"].values

    def test_status_lowercased(self, raw_orders):
        out = transform_orders(raw_orders)
        o3 = out.loc[out["order_id"] == "o3", "status"].iloc[0]
        assert o3 == "canceled"

    def test_delivery_days_positive(self, raw_orders):
        out = transform_orders(raw_orders)
        delivered = out.loc[out["delivery_days"].notna()]
        assert (delivered["delivery_days"] > 0).all()

    def test_is_late_flag(self, raw_orders):
        # o1: delivered 2023-01-15, estimated 2023-01-20 → not late
        out = transform_orders(raw_orders)
        o1_late = out.loc[out["order_id"] == "o1", "is_late"].iloc[0]
        assert o1_late is False or o1_late == False  # noqa: E712

    def test_timestamps_parsed(self, raw_orders):
        out = transform_orders(raw_orders)
        assert pd.api.types.is_datetime64_any_dtype(out["purchase_timestamp"])


# ──────────────────────────────────────────────
#  Order items
# ──────────────────────────────────────────────

class TestTransformOrderItems:
    def test_drops_negative_price(self, raw_order_items):
        # o1/item2 has price = -5.00
        out = transform_order_items(raw_order_items)
        neg = out[out["price"] < 0]
        assert len(neg) == 0

    def test_non_negative_freight(self, raw_order_items):
        out = transform_order_items(raw_order_items)
        assert (out["freight_value"] >= 0).all()

    def test_order_item_id_is_numeric(self, raw_order_items):
        out = transform_order_items(raw_order_items)
        assert pd.api.types.is_numeric_dtype(out["order_item_id"])


# ──────────────────────────────────────────────
#  Products
# ──────────────────────────────────────────────

class TestTransformProducts:
    def test_null_category_filled_with_unknown(self, raw_products):
        out = transform_products(raw_products)
        p2 = out.loc[out["product_id"] == "p2", "category"].iloc[0]
        assert p2 == "unknown"

    def test_empty_category_normalised(self, raw_products):
        out = transform_products(raw_products)
        p4 = out.loc[out["product_id"] == "p4", "category"].iloc[0]
        assert p4 == "unknown"

    def test_category_lowercased(self, raw_products):
        out = transform_products(raw_products)
        p1 = out.loc[out["product_id"] == "p1", "category"].iloc[0]
        assert p1 == "electronics"

    def test_negative_weight_nullified(self, raw_products):
        out = transform_products(raw_products)
        p2_weight = out.loc[out["product_id"] == "p2", "weight_g"].iloc[0]
        assert p2_weight is None or math.isnan(float(p2_weight))

    def test_non_numeric_weight_nullified(self, raw_products):
        out = transform_products(raw_products)
        p4_weight = out.loc[out["product_id"] == "p4", "weight_g"].iloc[0]
        assert p4_weight is None or math.isnan(float(p4_weight))
