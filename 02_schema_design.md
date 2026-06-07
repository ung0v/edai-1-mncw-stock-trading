# 02 Schema Design and Pipeline Implementation - Stock Trading Lakehouse

## 1. Goal

This section designs and implements a business-ready stock-trading Gold zone together with the full data pipeline needed to build it.

Objectives:

- ingest raw generated data
- store Bronze and Silver in lakehouse storage
- build Gold dimensions, facts, OBT, and feature tables
- orchestrate the flow end to end
- run data quality checks and record run metadata
- optionally emit lineage metadata through DataHub
- implement an offline analytical feature store, with online feature serving deferred to future work

Implemented approach:

- Spark for processing
- Delta Lake for Bronze, Silver, and Gold tables
- MinIO as S3-compatible object storage
- Airflow for orchestration
- Kafka for event transport
- PostgreSQL for curated exports

Current scope note:

- the implemented `feat_*` tables act as an offline feature store for analytics, model training support, and inspection
- an online feature store for low-latency serving is intentionally out of scope for the current implementation

Naming approach:

- Bronze tables are stored under `bronze/`
- Silver tables use `stg_` naming
- Gold tables use `dim_`, `fact_`, `obt_`, and `feat_` naming

---

## 2. Input Data Profile

### 2.1 Input Datasets

Offline source tables:

- `customers`
- `accounts`
- `securities`
- `orders`
- `trades`
- `cash_transactions`

Streaming source:

- `trading_events`

### 2.2 Estimated Volume

Configured baseline:

- `customers`: 50,000 rows
- `accounts`: 60,000 rows
- `securities`: 500 rows
- `orders`: 200,000 source rows plus duplicates
- `trades`: derived from executable orders
- `cash_transactions`: derived from trades and extra funding activity
- `trading_events`: generated over a trading-day timeline with burst windows

### 2.3 Velocity

- offline sources: batch generation
- stream source: minute-level event generation
- Kafka: optional transport before local JSONL consumption
- Airflow: scheduled or manually triggered orchestration

### 2.4 Key Data Characteristics

- high-cardinality identifiers:
  - `customer_id`
  - `account_id`
  - `order_id`
  - `trade_id`
  - `event_id`
- important event timestamps:
  - `order_timestamp`
  - `trade_timestamp`
  - `transaction_timestamp`
  - `event_timestamp`
- technical timestamps:
  - `created_ts`
  - `updated_ts`
  - `ingest_ts`
- schema evolution risk:
  - historical `orders.order_channel` may be null

### 2.5 Known Data Problems From Section 01

- skewed trading activity toward top securities
- duplicate orders
- duplicate stream events
- late-arriving events
- market open and close burst traffic
- historical missing `order_channel`

---

## 3. Storage and Layering Strategy

### 3.1 Raw Layer

Raw generated files are kept locally for inspection:

- `data/raw/offline/`
- `data/stream/trading_events/`

### 3.2 Bronze Layer

Bronze data is stored in Delta format on MinIO:

```text
s3a://stock-lakehouse/bronze/
```

Bronze datasets:

- `customers`
- `accounts`
- `securities`
- `orders`
- `trades`
- `cash_transactions`
- `trading_events`

Bronze strategy:

- append-only ingestion
- add `ingest_ts`
- add `batch_id`
- add `source_file`

Evidence screenshot:

![MinIO Bronze Silver Gold layout](./images/minio.png)

### 3.3 Silver Layer

Silver data is stored in Delta format on MinIO:

```text
s3a://stock-lakehouse/silver/
```

Silver tables:

- `stg_customers`
- `stg_accounts`
- `stg_securities`
- `stg_orders`
- `stg_trades`
- `stg_cash_transactions`
- `stg_trading_events`

Silver strategy:

- type standardisation
- timestamp normalisation
- deduplication by business key
- late-arrival flags for streaming events
- derived date columns for downstream modelling

### 3.4 Gold Layer

Gold data is also stored in Delta format on MinIO:

```text
s3a://stock-lakehouse/gold/
```

Gold outputs are also exported to PostgreSQL for easier inspection and BI-style querying.

---

## 4. Dimension Design

### 4.1 `dim_customer`

- grain: one row per customer
- keys: `customer_key` (SK), `customer_id` (BK)
- attributes:
  - customer profile fields
  - risk profile
  - KYC status
  - customer segment
  - geographic fields

### 4.2 `dim_account`

- grain: one row per account
- keys: `account_key` (SK), `account_id` (BK)
- attributes:
  - `customer_id`
  - account type
  - account status
  - opened/closed timestamps

### 4.3 `dim_security`

- grain: one row per security
- keys: `security_key` (SK), `security_id` (BK)
- attributes:
  - ticker
  - exchange
  - sector
  - security type
  - listed date
  - active flag

### 4.4 `dim_date`

- grain: one row per calendar date
- key: `date_key`
- attributes:
  - calendar date
  - day of week
  - month
  - quarter
  - year
  - weekend indicator

---

## 5. Fact Design

### 5.1 `fact_order`

- grain: one row per deduplicated business order
- keys:
  - `customer_key`
  - `account_key`
  - `security_key`
  - `order_date_key`
- measures:
  - `order_quantity`
  - `limit_price`
  - `order_amount`
- notes:
  - sourced from deduplicated Silver orders
  - historical null `order_channel` is preserved as schema evolution evidence

### 5.2 `fact_trade`

- grain: one row per trade
- keys:
  - `customer_key`
  - `account_key`
  - `security_key`
  - `trade_date_key`
- measures:
  - `trade_quantity`
  - `trade_price`
  - `trade_amount`
  - `fee_amount`

### 5.3 `fact_cash_transaction`

- grain: one row per cash transaction
- keys:
  - `customer_key`
  - `account_key`
  - `transaction_date_key`
- measures:
  - `amount`

---

## 6. OBT Design

### 6.1 `obt_customer_trading_activity`

- grain: one row per customer
- purpose: denormalised summary for reporting and quick inspection
- example measures:
  - total orders
  - buy order count
  - sell order count
  - total order amount
  - total trades
  - total trade amount
  - total fee amount
  - total cash transactions
  - net cash amount
  - first/last order and trade timestamps

---

## 7. Feature Store Design

### 7.1 `feat_customer_90d`

- grain: customer + feature timestamp
- inputs: `fact_order`, `fact_trade`
- measures:
  - customer order count
  - buy order count
  - sell order count
  - total order amount
  - average order amount
  - distinct ticker count
  - trade count
  - total trade amount
  - total fee amount

### 7.2 `feat_security_1d`

- grain: security + feature timestamp
- inputs: `fact_order`, `fact_trade`
- measures:
  - order count
  - buy/sell order count
  - total order amount
  - distinct customer count
  - trade count
  - total trade amount
  - average trade price

### 7.3 `feat_stream_customer_60m`

- grain: customer + feature timestamp
- input: `stg_trading_events`
- measures:
  - stream event count
  - order placed count
  - order matched count
  - price viewed count
  - distinct ticker count
  - late event count

### 7.4 `feat_customer_unified`

- grain: customer + feature timestamp
- purpose: join stable customer history with recent stream behaviour

### 7.5 Current Feature Store Boundary

- implemented today:
  - offline analytical feature tables in Gold
  - batch-style recomputation through Spark
  - feature export for downstream analytics and model support
- not implemented yet:
  - online feature store
  - low-latency feature retrieval for real-time inference
  - dedicated online/offline feature consistency layer

---

## 8. Data Pipeline Design and Implementation

### 8.1 Orchestration

Airflow DAG:

- `stock_trading_pipeline`

Actual task sequence:

1. `emit_datahub_metadata`
2. `generate_offline`
3. `generate_stream`
4. `produce_kafka_events`
5. `consume_kafka_events`
6. `generate_reports`
7. `bronze_ingestion`
8. `silver_transform`
9. `gold_transform`
10. `feature_transform`
11. `quality_checks`
12. `export_postgres`

Expected evidence for submission:

- screenshot of successful Airflow DAG run
- successful task logs for one core transformation stage

### 8.2 Pipeline Modules

| Module | Purpose |
|---|---|
| `src.pipelines.emit_datahub_metadata` | optional DataHub metadata emission |
| `src.pipelines.bronze_ingest` | raw to Bronze Delta |
| `src.pipelines.silver_transform` | Bronze to Silver cleaning and dedup |
| `src.pipelines.gold_transform` | Silver to Gold dimensions, facts, and OBT |
| `src.pipelines.feature_transform` | Gold and Silver to feature tables |
| `src.pipelines.quality_checks` | quality validation and run logs |
| `src.pipelines.export_postgres` | Gold export to PostgreSQL |

### 8.3 Update Strategy

- Bronze: append-only with ingest metadata
- Silver: full-table rebuild from Bronze with deterministic deduplication
- Gold: full-table rebuild from Silver
- Features: full-table rebuild from Gold and Silver
- Export: overwrite curated PostgreSQL tables

For coursework scale, deterministic full rebuilds are simpler to validate than incremental merge logic.

### 8.4 Late-Data Handling

- late stream events are identified in Silver using `created_ts` versus `event_timestamp`
- downstream feature logic can count or analyse those late events
- full rebuild strategy avoids complex partial-window reconciliation for this coursework scope

---

## 9. Refresh, Quality, and Monitoring

### 9.1 Practical Refresh Model

- raw generation: on demand or per DAG run
- Bronze/Silver/Gold/features: per DAG run
- PostgreSQL export: after quality checks

### 9.2 Implemented Quality Checks

- minimum row-volume checks
- uniqueness checks
- not-null checks
- referential integrity checks
- freshness checks

Quality evidence outputs:

- `outputs/reports/pipeline_quality_report.json`
- `outputs/run_logs/quality_checks_<run_id>.json`

Current implementation status:

- direct execution in the Airflow runtime completed successfully
- the latest verified quality run returned `PASS`

---

## 10. Lineage

Optional DataHub support is available through:

- `data-hub-docker-compose.yaml`

The DAG includes `emit_datahub_metadata` so the project can demonstrate lineage-oriented metadata emission when the overlay is enabled.

Conceptual lineage path:

- Raw -> Bronze -> Silver -> Gold -> Features -> PostgreSQL export

Current limitation:

- DataHub metadata emission is implemented
- however, the Airflow workflow is not yet appearing correctly in DataHub as a full pipeline lineage view
- this remains a known integration issue in the current project state

---

## 11. Warehouse Optimisation

Applied strategies:

- Delta Lake format for Bronze, Silver, and Gold
- MinIO object storage for compute/storage separation
- date-based partition support in downstream fact modelling
- deduplication before Gold fact construction

Observed trade-offs:

- full rebuilds are easy to reason about but not the most efficient
- Spark jobs are heavier than necessary for local resources
- some surrogate-key and feature computations can be optimised further

---

## 12. Achieved Implementation Scope

Implemented:

- generator
- raw data outputs
- Kafka event transport
- Bronze Delta ingestion on MinIO
- Silver Delta transformation on MinIO
- Gold Delta modelling on MinIO
- feature tables
- quality checks
- PostgreSQL export
- optional DataHub metadata emission

Verified by direct execution in the Airflow runtime.

Evidence screenshot for exported serving tables:

![Exported PostgreSQL tables](./images/exported_postgres.png)

---

## 13. Run Instructions

### 13.1 Start the Core Stack

```bash
make network-up
docker compose up -d --build
```

### 13.2 Trigger the DAG

- open `http://localhost:58080`
- log in with `airflow / airflow`
- enable `stock_trading_pipeline`
- trigger a manual run

### 13.3 Useful Manual Commands

```bash
python -m src.generator.generate_offline
python -m src.generator.generate_stream
python -m src.generator.generate_reports
python -m src.pipelines.bronze_ingest
python -m src.pipelines.silver_transform
python -m src.pipelines.gold_transform
python -m src.pipelines.feature_transform
python -m src.pipelines.quality_checks
DATABASE_URL=postgresql+psycopg2://postgres:postgres@postgres:5432/stock_dw python -m src.pipelines.export_postgres
```

---

## 14. Limitations and Next Steps

Current limitations:

- no incremental merge implementation yet
- no advanced point-in-time training join workflow beyond feature timestamps
- Gold and feature jobs are heavier than necessary for local resources
- the current feature store is offline only; online feature serving is future work
- full Airflow workflow visibility in DataHub is still incomplete even though metadata emission is present

Recommended next steps:

- optimise surrogate-key generation
- reduce repeated Spark scans in feature jobs
- add explicit lineage screenshots or exported evidence
- add incremental processing if later coursework phases require it
