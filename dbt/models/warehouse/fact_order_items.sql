{{
    config(
        materialized = 'table',
        schema       = 'warehouse'
    )
}}

/*
  fact_order_items — grain: one row per order line item.

  Joins:
    staging.order_items  (base)
    staging.orders       (status + timestamps + delivery metrics)
    staging.order_payments  (aggregated total per order)
    staging.order_reviews   (average score per order)
    dim_customer         (customer_key lookup)
    dim_product          (product_key lookup)
    dim_seller           (seller_key lookup)
    dim_date             (date_key lookup)
*/

WITH items AS (
    SELECT * FROM {{ source('silver', 'order_items') }}
),

orders AS (
    SELECT
        order_id,
        customer_id,
        status,
        purchase_timestamp,
        delivery_days,
        is_late
    FROM {{ source('silver', 'orders') }}
),

payments AS (
    SELECT
        order_id,
        SUM(payment_value) AS total_payment
    FROM {{ source('silver', 'order_payments') }}
    GROUP BY order_id
),

reviews AS (
    SELECT
        order_id,
        ROUND(AVG(score), 0)::SMALLINT AS review_score
    FROM {{ source('silver', 'order_reviews') }}
    GROUP BY order_id
)

SELECT
    i.order_id,
    i.order_item_id,

    -- Dimension keys (natural business keys)
    o.customer_id                                                       AS customer_key,
    i.product_id                                                        AS product_key,
    i.seller_id                                                         AS seller_key,
    TO_CHAR(o.purchase_timestamp, 'YYYYMMDD')::INT                     AS date_key,

    -- Facts
    o.status                                                            AS order_status,
    i.price,
    i.freight_value,
    pay.total_payment,
    rev.review_score,
    o.delivery_days,
    o.is_late,

    -- Convenience partitions (avoids joins for simple time-series queries)
    EXTRACT(YEAR  FROM o.purchase_timestamp)::SMALLINT                 AS purchase_year,
    EXTRACT(MONTH FROM o.purchase_timestamp)::SMALLINT                 AS purchase_month

FROM items i
LEFT JOIN orders  o   ON i.order_id   = o.order_id
LEFT JOIN payments pay ON i.order_id  = pay.order_id
LEFT JOIN reviews  rev ON i.order_id  = rev.order_id
