-- ============================================================
--  DataOps  –  Three-layer schema
--  raw       : Bronze  – exact replica of source data
--  staging   : Silver  – cleaned, typed, validated
--  warehouse : Gold    – star schema for analytics
--  meta      : Pipeline tracking (watermarks, run logs)
-- ============================================================

-- ──────────────────────────────────────────────
--  Schema creation
-- ──────────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS warehouse;
CREATE SCHEMA IF NOT EXISTS meta;

-- ──────────────────────────────────────────────
--  BRONZE  (raw)  –  no types enforced, text only
-- ──────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS raw.customers (
    customer_id         TEXT,
    customer_unique_id  TEXT,
    zip_code            TEXT,
    city                TEXT,
    state               TEXT,
    _loaded_at          TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw.sellers (
    seller_id   TEXT,
    zip_code    TEXT,
    city        TEXT,
    state       TEXT,
    _loaded_at  TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw.products (
    product_id      TEXT,
    category        TEXT,
    weight_g        TEXT,
    length_cm       TEXT,
    height_cm       TEXT,
    width_cm        TEXT,
    _loaded_at      TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw.orders (
    order_id                    TEXT,
    customer_id                 TEXT,
    status                      TEXT,
    purchase_timestamp          TEXT,
    approved_at                 TEXT,
    delivered_carrier_date      TEXT,
    delivered_customer_date     TEXT,
    estimated_delivery_date     TEXT,
    _loaded_at                  TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw.order_items (
    order_id        TEXT,
    order_item_id   TEXT,
    product_id      TEXT,
    seller_id       TEXT,
    shipping_limit  TEXT,
    price           TEXT,
    freight_value   TEXT,
    _loaded_at      TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw.order_payments (
    order_id                TEXT,
    payment_sequential      TEXT,
    payment_type            TEXT,
    payment_installments    TEXT,
    payment_value           TEXT,
    _loaded_at              TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw.order_reviews (
    review_id           TEXT,
    order_id            TEXT,
    score               TEXT,
    comment_title       TEXT,
    comment_message     TEXT,
    creation_date       TEXT,
    answer_date         TEXT,
    _loaded_at          TIMESTAMPTZ DEFAULT now()
);

-- ──────────────────────────────────────────────
--  SILVER  (staging)  –  typed and cleaned
-- ──────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS staging.customers (
    customer_id         TEXT PRIMARY KEY,
    customer_unique_id  TEXT NOT NULL,
    zip_code            TEXT,
    city                TEXT,
    state               CHAR(2),
    _validated_at       TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS staging.sellers (
    seller_id   TEXT PRIMARY KEY,
    zip_code    TEXT,
    city        TEXT,
    state       CHAR(2),
    _validated_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS staging.products (
    product_id  TEXT PRIMARY KEY,
    category    TEXT NOT NULL DEFAULT 'unknown',
    weight_g    NUMERIC,
    length_cm   NUMERIC,
    height_cm   NUMERIC,
    width_cm    NUMERIC,
    _validated_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS staging.orders (
    order_id                    TEXT PRIMARY KEY,
    customer_id                 TEXT NOT NULL,
    status                      TEXT NOT NULL,
    purchase_timestamp          TIMESTAMPTZ,
    approved_at                 TIMESTAMPTZ,
    delivered_carrier_date      TIMESTAMPTZ,
    delivered_customer_date     TIMESTAMPTZ,
    estimated_delivery_date     TIMESTAMPTZ,
    delivery_days               INT,
    is_late                     BOOLEAN DEFAULT FALSE,
    _validated_at               TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS staging.order_items (
    order_id        TEXT NOT NULL,
    order_item_id   INT NOT NULL,
    product_id      TEXT NOT NULL,
    seller_id       TEXT NOT NULL,
    shipping_limit  TIMESTAMPTZ,
    price           NUMERIC(12,2) NOT NULL,
    freight_value   NUMERIC(12,2) NOT NULL,
    _validated_at   TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (order_id, order_item_id)
);

CREATE TABLE IF NOT EXISTS staging.order_payments (
    order_id                TEXT NOT NULL,
    payment_sequential      INT NOT NULL,
    payment_type            TEXT NOT NULL,
    payment_installments    INT,
    payment_value           NUMERIC(12,2) NOT NULL,
    _validated_at           TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (order_id, payment_sequential)
);

CREATE TABLE IF NOT EXISTS staging.order_reviews (
    review_id       TEXT PRIMARY KEY,
    order_id        TEXT NOT NULL,
    score           SMALLINT CHECK (score BETWEEN 1 AND 5),
    comment_title   TEXT,
    comment_message TEXT,
    creation_date   TIMESTAMPTZ,
    answer_date     TIMESTAMPTZ,
    _validated_at   TIMESTAMPTZ DEFAULT now()
);

-- ──────────────────────────────────────────────
--  GOLD  (warehouse)  –  star schema
-- ──────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS warehouse.dim_date (
    date_key    INT PRIMARY KEY,
    full_date   DATE NOT NULL,
    day         SMALLINT,
    month       SMALLINT,
    year        SMALLINT,
    quarter     SMALLINT,
    day_of_week SMALLINT,
    day_name    TEXT,
    month_name  TEXT,
    is_weekend  BOOLEAN
);

CREATE TABLE IF NOT EXISTS warehouse.dim_customer (
    customer_key        SERIAL PRIMARY KEY,
    customer_id         TEXT UNIQUE NOT NULL,
    customer_unique_id  TEXT,
    city                TEXT,
    state               CHAR(2),
    _loaded_at          TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS warehouse.dim_product (
    product_key SERIAL PRIMARY KEY,
    product_id  TEXT UNIQUE NOT NULL,
    category    TEXT,
    weight_g    NUMERIC,
    _loaded_at  TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS warehouse.dim_seller (
    seller_key  SERIAL PRIMARY KEY,
    seller_id   TEXT UNIQUE NOT NULL,
    city        TEXT,
    state       CHAR(2),
    _loaded_at  TIMESTAMPTZ DEFAULT now()
);

-- Grain: one row per order line item
CREATE TABLE IF NOT EXISTS warehouse.fact_order_items (
    item_key            BIGSERIAL PRIMARY KEY,
    order_id            TEXT NOT NULL,
    order_item_id       INT NOT NULL,
    customer_key        INT REFERENCES warehouse.dim_customer(customer_key),
    product_key         INT REFERENCES warehouse.dim_product(product_key),
    seller_key          INT REFERENCES warehouse.dim_seller(seller_key),
    date_key            INT REFERENCES warehouse.dim_date(date_key),
    order_status        TEXT,
    price               NUMERIC(12,2),
    freight_value       NUMERIC(12,2),
    total_payment       NUMERIC(12,2),
    review_score        SMALLINT,
    delivery_days       INT,
    is_late             BOOLEAN,
    purchase_year       SMALLINT,
    purchase_month      SMALLINT,
    UNIQUE (order_id, order_item_id)
);

-- ──────────────────────────────────────────────
--  META  –  pipeline tracking
-- ──────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS meta.watermarks (
    entity          TEXT PRIMARY KEY,
    last_loaded_at  TIMESTAMPTZ NOT NULL DEFAULT '1970-01-01'
);

CREATE TABLE IF NOT EXISTS meta.pipeline_runs (
    run_id          SERIAL PRIMARY KEY,
    dag_id          TEXT NOT NULL,
    task_id         TEXT NOT NULL,
    status          TEXT NOT NULL,
    rows_processed  INT DEFAULT 0,
    started_at      TIMESTAMPTZ DEFAULT now(),
    finished_at     TIMESTAMPTZ,
    error_message   TEXT
);

-- Seed watermarks
INSERT INTO meta.watermarks (entity, last_loaded_at)
VALUES
    ('customers',       '1970-01-01'),
    ('sellers',         '1970-01-01'),
    ('products',        '1970-01-01'),
    ('orders',          '1970-01-01'),
    ('order_items',     '1970-01-01'),
    ('order_payments',  '1970-01-01'),
    ('order_reviews',   '1970-01-01')
ON CONFLICT (entity) DO NOTHING;
