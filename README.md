# StreamCart — Real-Time E-Commerce Analytics Pipeline

A production-grade, end-to-end data engineering project that processes e-commerce order data in real-time using a medallion architecture, orchestrated with Apache Airflow.

---

## Architecture
[Olist Dataset]
│
▼
[Apache Kafka]  ◄─── orders_producer.py (99K+ events replayed)
│
▼
[Spark Structured Streaming]
│
▼
[AWS S3 — Bronze Layer]  ← Raw Parquet files (895 files, append-only)
│
▼
[Spark Batch — Silver Layer]  ← Dedup, null handling, JSON parse, type casting
│
▼
[Great Expectations]  ← 10 automated data quality checks
│
▼
[Spark Batch — Gold Layer]  ← Star Schema loaded into PostgreSQL
│
▼
[PostgreSQL — Star Schema]  ← fact_orders, dim_customer, dim_product...
│
▼
[Power BI Dashboard]  ← Sales KPIs, conversion funnel, inventory alerts
Orchestration: Apache Airflow (DAG: Silver → Quality → Gold, @daily)
Infrastructure: Docker Compose (single command setup)

---

## Tech Stack

| Layer | Technology |
|-------|------------|
| Ingestion | Apache Kafka 7.5.0, kafka-python |
| Stream Processing | Apache Spark 4.1.1 Structured Streaming |
| Batch Processing | Apache Spark 4.1.1 |
| Data Lake | AWS S3 (Bronze / Silver layers) |
| Data Warehouse | PostgreSQL 15 via AWS RDS (Gold layer) |
| Orchestration | Apache Airflow 2.8.1 |
| Data Quality | Great Expectations 0.18.19 |
| Infrastructure | Docker Compose |
| Language | Python 3.12 |

---

## Data Model — Gold Layer (Star Schema)
fact_orders (99,441 rows)
├── dim_customer (99,441 rows)
├── dim_product  (32,951 rows)
├── dim_seller   ( 3,095 rows)
└── dim_date     (   634 rows)
fact_order_items (112,650 rows)

---

## Quick Start

### Prerequisites
- Docker Desktop
- Python 3.12+
- Apache Spark 4.1.1
- AWS account (S3 access)

### Setup

```bash
# 1. Clone the repository
git clone https://github.com/MustafaAygunDs/streamcart-pipeline.git
cd streamcart-pipeline

# 2. Configure environment variables
cp .env.example .env
# Fill in your AWS credentials and database password

# 3. Install dependencies
pip install -r requirements.txt

# 4. Start all services
make up

# 5. Run the full pipeline
make etl

# 6. Run data quality checks
make test
```

### Airflow UI
Navigate to **http://localhost:8081** — login with `admin / admin`

---

## Pipeline Layers

### Bronze — Streaming Ingestion
Reads order events from Kafka and writes raw Parquet files to S3 (append-only, immutable).

```bash
make bronze   # Start Spark Structured Streaming job
```

- Source: Apache Kafka topic `olist.orders`
- Sink: `s3://streamcart-data-lake/bronze/streaming/orders/`
- Format: Snappy-compressed Parquet
- Output: 895 Parquet files

### Silver — Batch Transformation
Cleans and enriches the Bronze data.

- JSON payload parsing and schema enforcement
- Deduplication: 99,451 → 99,441 records (10 duplicates removed)
- Null handling on critical columns
- Timestamp normalization using `try_to_timestamp` (NaN-safe)
- Partitioned by `order_status`

### Data Quality
10 automated checks via Great Expectations before loading to Gold.

| Check | Result |
|-------|--------|
| order_id NOT NULL | ✅ |
| customer_id NOT NULL | ✅ |
| order_id UNIQUE | ✅ |
| order_status valid set | ✅ |
| Row count between 90K–110K | ✅ |
| order_purchase_timestamp NOT NULL | ✅ |
| dim_customer: customer_id NOT NULL | ✅ |
| dim_customer: customer_id UNIQUE | ✅ |
| dim_customer: customer_state NOT NULL | ✅ |
| dim_customer row count | ✅ |

```bash
make test   # Run all quality checks
```

### Gold — Star Schema Load
Loads cleaned Silver data into PostgreSQL using Kimball star schema.

```bash
make etl   # Run Silver + Gold pipeline
```

---

## Key Technical Decisions

**Medallion Architecture (Bronze / Silver / Gold)**
Each layer is immutable and independently replayable. Raw data is always preserved in Bronze, enabling full reprocessing without re-ingestion.

**Hybrid Ingestion (Streaming + Batch)**
Clickstream and order events use Kafka → Spark Streaming (low latency). Dimension data (products, sellers) uses batch CDC — matching real-world e-commerce platform patterns used by Trendyol and Getir.

**try_to_timestamp over to_timestamp**
Olist's real dataset contains `NaN` values in date columns. Using `try_to_timestamp` converts malformed inputs to NULL instead of failing — this is the production-safe approach.

**Docker Compose Single-Command Setup**
`make up` starts Kafka, Zookeeper, PostgreSQL, and Airflow. A recruiter or interviewer can have the full stack running in under 2 minutes.

**Airflow with Separate Database**
Airflow metadata is isolated in its own `airflow_db` database, preventing schema conflicts with the StreamCart `streamcart` database.

---

## Project Structure
streamcart-pipeline/
├── ingestion/
│   └── producers/
│       └── orders_producer.py      # Kafka event replay producer
├── processing/
│   ├── bronze/
│   │   └── orders_bronze.py        # Spark Structured Streaming job
│   ├── silver/
│   │   └── orders_silver.py        # Batch transformation + dedup
│   └── gold/
│       └── orders_gold.py          # Star schema loader
├── orchestration/
│   └── dags/
│       └── streamcart_pipeline.py  # Airflow DAG
├── quality/
│   └── expectations/
│       └── orders_expectations.py  # Great Expectations suite
├── scripts/
│   ├── upload_to_s3.py             # Initial data upload
│   └── create_gold_schema.py       # DDL script
├── docker-compose.yml
├── Makefile
├── requirements.txt
└── .env.example

---

## Performance Metrics

| Metric | Value |
|--------|-------|
| Total orders processed | 99,441 |
| Bronze Parquet files | 895 |
| Duplicates removed (Silver) | 10 |
| Gold tables | 6 |
| Data quality score | 10/10 ✅ |
| Kafka events replayed | 99,441 orders + 112,650 items |

---

## About

**Mustafa Aygün** — Data Engineer
- GitHub: [MustafaAygunDs](https://github.com/MustafaAygunDs)
- LinkedIn: [mustafaaygunds](https://www.linkedin.com/in/mustafaaygunds/)
