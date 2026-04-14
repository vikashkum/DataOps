# DataOps — End-to-End E-Commerce Data Pipeline

A production-style ETL pipeline demonstrating Python, SQL, Airflow, Docker, and BI tooling on a synthetic e-commerce dataset.

---

## Architecture

```
CSV Files (data/raw/)
      │
      ▼
┌─────────────┐     ┌─────────────────────────────────────────────┐
│  Airflow    │────▶│           PostgreSQL (dataops DB)            │
│  Scheduler  │     │                                              │
│  (daily)    │     │  raw.*       Bronze  – exact CSV copy        │
└─────────────┘     │  staging.*   Silver  – cleaned + typed       │
      │             │  warehouse.* Gold    – star schema            │
      │             │  meta.*              – watermarks + run log   │
      ▼             └─────────────────────────────────────────────┘
 DAG Tasks:                        │
  1. check_source_files             ▼
  2. extract_to_bronze        ┌──────────┐
  3. transform_to_silver      │ Metabase │  → Dashboards
  4. load_to_gold             └──────────┘
```

## Star Schema (Gold Layer)

```
              dim_date
                 │
dim_customer ────┤
                 ├──── fact_order_items ──── dim_product
dim_seller   ────┤
```

**Fact grain:** one row per order line item.

---

## Tech Stack

| Component      | Technology           |
|----------------|----------------------|
| Orchestration  | Apache Airflow 2.8   |
| Processing     | Python 3.11 + Pandas |
| Storage        | PostgreSQL 15        |
| Visualisation  | Metabase 0.48        |
| Containers     | Docker + Compose     |
| CI/CD          | GitHub Actions       |

---

## Prerequisites

- Docker Desktop (running)
- Python 3.11 (for local dev / CI)
- Git + a GitHub Actions self-hosted runner (for CI)

---

## Quick Start

### 1. Clone and configure

```bash
git clone <your-repo-url>
cd DataOps
cp .env.example .env
```

### 2. Generate synthetic data

```bash
pip install -r requirements.txt
python scripts/generate_data.py
# Creates data/raw/*.csv  (~50 k orders, 10 k customers, …)
```

### 3. Start the stack

```bash
docker compose up --build -d
```

Services:

| Service           | URL                    | Credentials      |
|-------------------|------------------------|------------------|
| Airflow UI        | http://localhost:8080  | admin / admin    |
| Metabase          | http://localhost:3000  | (first-run setup)|
| PostgreSQL        | localhost:5432         | dataops / dataops|

> First startup takes a few minutes while the Airflow image builds and `airflow db migrate` runs.

### 4. Run the pipeline

**Option A — Airflow UI**

1. Open http://localhost:8080
2. Enable the `ecommerce_etl` DAG (toggle on)
3. Click **Trigger DAG**

**Option B — CLI**

```bash
docker compose exec airflow-scheduler \
  airflow dags trigger ecommerce_etl
```

### 5. Connect Metabase

1. Open http://localhost:3000 and complete the setup wizard.
2. Add database:
   - Type: **PostgreSQL**
   - Host: `postgres`   Port: `5432`
   - Database: `dataops`  User/Password: `dataops`
3. Browse **warehouse** schema to build dashboards.

---

## Project Structure

```
DataOps/
├── .github/workflows/ci.yml     # GitHub Actions (lint + test + DAG check)
├── dags/
│   └── etl_pipeline.py          # Airflow DAG definition
├── scripts/
│   ├── db.py                    # DB connection + helpers
│   ├── generate_data.py         # Synthetic data generator
│   ├── extract.py               # Bronze layer (CSV → raw.*)
│   ├── transform.py             # Silver layer (raw.* → staging.*)
│   └── load.py                  # Gold layer  (staging.* → warehouse.*)
├── sql/
│   ├── init.sql                 # Creates 'airflow' database on first boot
│   └── schema.sql               # All DDL (raw / staging / warehouse / meta)
├── tests/
│   ├── conftest.py
│   ├── test_transform.py        # Unit tests – Silver transformations
│   └── test_load.py             # Unit tests – Gold dim builders
├── data/raw/                    # Generated CSVs (git-ignored)
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
└── .env.example
```

---

## Sample Analytical Queries

**Daily revenue trend**
```sql
SELECT
    d.full_date,
    SUM(f.price + f.freight_value) AS revenue,
    COUNT(DISTINCT f.order_id)     AS orders
FROM warehouse.fact_order_items f
JOIN warehouse.dim_date d ON f.date_key = d.date_key
WHERE f.order_status = 'delivered'
GROUP BY d.full_date
ORDER BY d.full_date;
```

**Revenue by product category**
```sql
SELECT
    p.category,
    ROUND(SUM(f.price)::numeric, 2) AS total_revenue,
    COUNT(*)                         AS line_items
FROM warehouse.fact_order_items f
JOIN warehouse.dim_product p ON f.product_key = p.product_key
GROUP BY p.category
ORDER BY total_revenue DESC;
```

**Top 10 states by order volume**
```sql
SELECT
    c.state,
    COUNT(*)                        AS orders,
    ROUND(AVG(f.review_score), 2)   AS avg_review
FROM warehouse.fact_order_items f
JOIN warehouse.dim_customer c ON f.customer_key = c.customer_key
WHERE f.review_score IS NOT NULL
GROUP BY c.state
ORDER BY orders DESC
LIMIT 10;
```

**Late delivery rate by month**
```sql
SELECT
    d.year,
    d.month,
    COUNT(*)                                     AS total_delivered,
    SUM(CASE WHEN f.is_late THEN 1 ELSE 0 END)  AS late_count,
    ROUND(100.0 * SUM(CASE WHEN f.is_late THEN 1 ELSE 0 END)
          / COUNT(*), 1)                         AS late_pct
FROM warehouse.fact_order_items f
JOIN warehouse.dim_date d ON f.date_key = d.date_key
WHERE f.order_status = 'delivered'
GROUP BY d.year, d.month
ORDER BY d.year, d.month;
```

---

## Running Tests Locally

```bash
pip install -r requirements.txt
pytest tests/ -v --cov=scripts --cov-report=term-missing
```

---

## Pipeline Features

| Feature               | Implementation                                  |
|-----------------------|-------------------------------------------------|
| Incremental loading   | `meta.watermarks` table tracks last load time   |
| Idempotency           | All inserts use `ON CONFLICT DO NOTHING/UPDATE` |
| Error handling        | Try/except per entity with `meta.pipeline_runs` |
| Run logging           | `meta.pipeline_runs` records every task attempt |
| Data validation       | Type coercion, range checks, constraint checks  |
| Star schema           | dim_date, dim_customer, dim_product, dim_seller |

---

## Stopping / Resetting

```bash
# Stop containers (keep data)
docker compose down

# Full reset (destroys all data volumes)
docker compose down -v
```

---

## Complete Start-to-End Runbook

Use this checklist every time you run the project from scratch.

### Step 1 — Clone and configure
```bash
git clone <your-repo-url>
cd DataOps
cp .env.example .env
```

### Step 2 — Install Python dependencies
```bash
pip install -r requirements.txt
```

### Step 3 — Generate synthetic data
```bash
python scripts/generate_data.py
```
Expected: 7 CSV files created in `data/raw/`
```
customers.csv   →  10,000 rows
sellers.csv     →     500 rows
products.csv    →   1,200 rows
orders.csv      →  50,000 rows
order_items.csv →  ~79,000 rows
order_payments  →  ~55,000 rows
order_reviews   →  ~35,000 rows
```

### Step 4 — Start all containers
```bash
docker compose up --build -d
```
Verify all 4 services are healthy:
```bash
docker compose ps
```

### Step 5 — Trigger the pipeline
1. Open **http://localhost:8080** → login `admin` / `admin`
2. Toggle `ecommerce_etl` **on** (grey → blue)
3. Click **▶** → **Trigger DAG** → **Trigger**
4. Wait for all 4 task boxes to turn **green**:
   ```
   check_source_files → extract_to_bronze → transform_to_silver → load_to_gold
   ```

### Step 6 — Verify data in PostgreSQL
```bash
docker compose exec postgres psql -U dataops -d dataops -c \
  "SELECT COUNT(*) FROM warehouse.fact_order_items;"
# Expected: 79757
```

### Step 7 — Open Metabase dashboards
Open **http://localhost:3000**

First-time setup — add database:
- Type: **PostgreSQL**
- Host: `postgres` / Port: `5432`
- Database: `dataops` / User+Password: `dataops`

### Step 8 — Run tests
```bash
pytest tests/ -v --cov=scripts --cov-report=term-missing
```

### Step 9 — Commit and push
```bash
git add .
git commit -m "Add complete e-commerce ETL pipeline"
git push origin main
```

---

## Dashboard SQL Queries

**Revenue by category**
```sql
SELECT p.category,
       ROUND(SUM(f.price)::numeric, 0) AS revenue,
       COUNT(*) AS line_items
FROM warehouse.fact_order_items f
JOIN warehouse.dim_product p ON f.product_key = p.product_key
GROUP BY p.category ORDER BY revenue DESC;
```

**Monthly revenue trend**
```sql
SELECT purchase_year || '-' || LPAD(purchase_month::text, 2, '0') AS month,
       ROUND(SUM(price)::numeric, 0) AS revenue
FROM warehouse.fact_order_items
WHERE order_status = 'delivered'
GROUP BY purchase_year, purchase_month
ORDER BY purchase_year, purchase_month;
```

**Late delivery rate by state**
```sql
SELECT c.state,
       COUNT(*) AS total_orders,
       ROUND(100.0 * SUM(CASE WHEN f.is_late THEN 1 ELSE 0 END) / COUNT(*), 1) AS late_pct
FROM warehouse.fact_order_items f
JOIN warehouse.dim_customer c ON f.customer_key = c.customer_key
WHERE f.order_status = 'delivered'
GROUP BY c.state ORDER BY late_pct DESC;
```

**Average review score by category**
```sql
SELECT p.category,
       ROUND(AVG(f.review_score)::numeric, 2) AS avg_score,
       COUNT(*) AS reviews
FROM warehouse.fact_order_items f
JOIN warehouse.dim_product p ON f.product_key = p.product_key
WHERE f.review_score IS NOT NULL
GROUP BY p.category ORDER BY avg_score DESC;
```

---

## Interview Talking Points

| What you built | How to explain it |
|---|---|
| 3-layer architecture | "Bronze/Silver/Gold medallion pattern — same as Databricks / Azure Data Factory" |
| Airflow DAG | "Orchestrated with retry logic, dependency chains, and run logging to a metadata table" |
| Star schema | "Fact + 4 dimensions, grain is one row per order line item, optimised for analytical queries" |
| Docker Compose | "Fully containerised — one command spins up Airflow, PostgreSQL, and Metabase" |
| Idempotency | "All inserts use ON CONFLICT so the pipeline can safely re-run without duplicating data" |
| CI/CD | "GitHub Actions self-hosted runner runs flake8 + pytest on every push" |