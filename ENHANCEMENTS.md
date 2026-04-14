# Enhancement Roadmap

All enhancements use free / open-source tools only.
Work on each enhancement in its own feature branch so `main` stays clean.

```
main  (always working)
 ├── features/enhancement01  ← dbt Core         (you are here)
 ├── features/enhancement02  ← Great Expectations
 ├── features/enhancement03  ← PySpark
 └── features/enhancement04  ← Apache Kafka
```

Merge each branch into `main` only after it runs end-to-end successfully.

---

## Enhancement 01 — dbt Core (Gold layer SQL models)

**Branch:** `features/enhancement01`
**Tool:** dbt-core + dbt-postgres (free, open source)
**What it replaces:** `scripts/load.py` Gold layer transforms

### Why
- dbt is mentioned in almost every data engineering job posting
- Gives you SQL lineage, auto-documentation, and built-in data tests
- Industry standard for the transformation layer (the T in ELT)

### What you will learn
- dbt models (`.sql` files that replace hand-written Python)
- dbt tests (`not_null`, `unique`, `accepted_values`, `relationships`)
- dbt docs (`dbt docs generate && dbt docs serve`)
- How to run dbt from inside an Airflow DAG

### Files to add
```
dbt/
├── dbt_project.yml              # project config
├── profiles.yml                 # DB connection
├── models/
│   ├── staging/                 # Silver → cleaned views
│   │   ├── stg_orders.sql
│   │   ├── stg_customers.sql
│   │   ├── stg_products.sql
│   │   └── stg_order_items.sql
│   └── warehouse/               # Gold → star schema
│       ├── dim_date.sql
│       ├── dim_customer.sql
│       ├── dim_product.sql
│       ├── dim_seller.sql
│       └── fact_order_items.sql
├── tests/
│   └── generic_tests.yml        # not_null, unique, FK checks
└── macros/
    └── generate_surrogate_key.sql
```

### Steps
1. `pip install dbt-core dbt-postgres`
2. `mkdir dbt && cd dbt && dbt init dataops`
3. Write models (SQL files)
4. `dbt run` — builds warehouse.* tables
5. `dbt test` — validates data quality
6. `dbt docs generate && dbt docs serve` — browse lineage at localhost:8080
7. Add a `dbt run` task to the Airflow DAG after `transform_to_silver`

### Verify it works
```bash
dbt run --select warehouse.*
dbt test
# All models green, all tests pass
```

---

## Enhancement 02 — Great Expectations (Data Quality)

**Branch:** `features/enhancement02`
**Tool:** great-expectations (free, open source)
**What it adds:** A validation task between Silver and Gold

### Why
- Shows you think about data quality, not just data movement
- Every senior data engineering role asks "how do you handle bad data?"
- Generates HTML validation reports you can show in interviews

### What you will learn
- Expectation suites (define what "good data" looks like)
- Validation runs (check data against expectations)
- Data docs (auto-generated HTML quality report)
- How to fail a pipeline when data quality drops below threshold

### Files to add
```
great_expectations/
├── great_expectations.yml       # config
├── expectations/
│   ├── orders_suite.json        # rules for orders table
│   ├── customers_suite.json
│   └── order_items_suite.json
└── checkpoints/
    └── daily_checkpoint.yml     # runs all suites together
```

### Key expectations to define
```python
# orders must have valid statuses
expect_column_values_to_be_in_set("status", ["delivered","shipped","processing","canceled"])

# price must be positive
expect_column_values_to_be_between("price", min_value=0.01)

# order_id must never be null
expect_column_values_to_not_be_null("order_id")

# review score must be 1-5
expect_column_values_to_be_between("score", min_value=1, max_value=5)
```

### Steps
1. `pip install great-expectations`
2. `great_expectations init`
3. Define expectation suites for orders, customers, order_items
4. Write a `scripts/validate.py` that runs checkpoints
5. Add `validate_silver` task to Airflow DAG between transform and load
6. `great_expectations docs build` — generates HTML report

### Updated DAG
```
extract_to_bronze → transform_to_silver → validate_silver → load_to_gold
```

### Verify it works
```bash
python scripts/validate.py
# Validation report generated at great_expectations/uncommitted/data_docs/
```

---

## Enhancement 03 — PySpark (Big Data Processing)

**Branch:** `features/enhancement03`
**Tool:** pyspark (free, open source, runs locally)
**What it replaces:** Pandas in `scripts/transform.py`

### Why
- Shows you can process data at scale (not just on a single machine)
- PySpark is used at most large companies (Databricks, Azure Synapse, AWS EMR)
- Same code runs locally AND on a cluster — huge resume signal

### What you will learn
- SparkSession, DataFrames, transformations
- Lazy evaluation (Spark doesn't run until `.show()` or `.write()`)
- Partitioning — how Spark splits data across workers
- Reading/writing from PostgreSQL via JDBC

### Files to add / modify
```
scripts/
└── transform_spark.py           # PySpark version of transform.py

config/
└── spark-defaults.conf          # Spark JVM config
```

### Key difference from Pandas
```python
# Pandas (current)
df["category"] = df["category"].fillna("unknown")

# PySpark (new)
from pyspark.sql.functions import col, when
df = df.withColumn("category",
    when(col("category").isNull(), "unknown").otherwise(col("category"))
)
```

### Steps
1. `pip install pyspark`
2. Create `scripts/transform_spark.py` with SparkSession
3. Replicate all 7 Silver transformations using Spark DataFrames
4. Write output to staging schema via JDBC
5. Add a `USE_SPARK=true` env variable to switch between Pandas and Spark
6. Update Airflow DAG to use the Spark version

### Verify it works
```bash
python scripts/transform_spark.py
# Check staging.* tables are populated
```

---

## Enhancement 04 — Apache Kafka (Streaming Pipeline)

**Branch:** `features/enhancement04`
**Tool:** Apache Kafka via Confluent Community (free, open source)
**What it adds:** A second real-time pipeline alongside the existing batch pipeline

### Why
- Shows you understand both batch AND streaming — most companies need both
- Kafka is the industry standard for event streaming
- Strongly asked about in data engineering interviews

### What you will learn
- Kafka producers (publish order events to a topic)
- Kafka consumers (read events and load to PostgreSQL)
- Topics, partitions, consumer groups
- Stream vs batch trade-offs

### Files to add
```
streaming/
├── producer.py                  # simulates real-time order events
├── consumer.py                  # reads events → loads to raw.orders_stream
└── kafka_config.py              # topic and connection config

dags/
└── streaming_monitor.py         # Airflow DAG that monitors consumer lag
```

### Architecture
```
producer.py → Kafka topic (orders_live) → consumer.py → raw.orders_stream
                                                              ↓
                                                    transform → warehouse
```

### docker-compose additions
```yaml
zookeeper:
  image: confluentinc/cp-zookeeper:7.5.0

kafka:
  image: confluentinc/cp-kafka:7.5.0
  depends_on: [zookeeper]
  ports:
    - "9092:9092"

kafka-ui:
  image: provectuslabs/kafka-ui:latest   # free UI for Kafka
  ports:
    - "9000:8080"
```

### Steps
1. Add Kafka + Zookeeper + Kafka-UI to docker-compose.yml
2. `pip install kafka-python`
3. Write `streaming/producer.py` — generates fake order events every second
4. Write `streaming/consumer.py` — reads events and inserts to PostgreSQL
5. Run producer and consumer in separate terminals
6. Watch events flow in Kafka-UI at http://localhost:9000
7. Add consumer lag monitoring to Airflow

### Verify it works
```bash
python streaming/producer.py &    # runs in background
python streaming/consumer.py      # reads events live
# Check raw.orders_stream table growing in real time
```

---

## Branch strategy

```bash
# Start each enhancement from main
git checkout main
git checkout -b features/enhancement02   # after 01 is merged

# Work on it, then merge back when done
git checkout main
git merge features/enhancement02
git push origin main
```

## Completion checklist per enhancement

- [ ] All existing tests still pass (`pytest tests/ -v`)
- [ ] Lint passes (`flake8 scripts/ dags/`)
- [ ] New feature runs end-to-end without errors
- [ ] README updated with new steps
- [ ] Committed and pushed to feature branch
- [ ] Merged to main
