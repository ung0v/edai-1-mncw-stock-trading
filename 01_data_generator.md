# 01 Data Generator - Stock Trading Platform

## 1. Domain Overview

This project simulates a stock-trading platform and generates both historical batch data and near-real-time activity data for downstream lakehouse processing.

The generator produces:

- offline historical/reference datasets in Parquet
- streaming event data in JSON Lines

The goal is not only to generate data, but also to intentionally introduce realistic data quality and processing challenges so later Bronze, Silver, Gold, and feature pipelines can demonstrate cleaning, deduplication, schema evolution handling, and quality monitoring.

---

## 2. Offline Dataset Design

Offline source files are written under:

```text
data/raw/offline/
```

### 2.1 Offline Tables

| Table               | Grain                           | Key Columns                                                                                |
| ------------------- | ------------------------------- | ------------------------------------------------------------------------------------------ |
| `customers`         | one row per customer            | `customer_id`, `signup_ts`, `risk_profile`, `kyc_status`                                   |
| `accounts`          | one row per account             | `account_id`, `customer_id`, `account_type`, `account_status`, `opened_ts`                 |
| `securities`        | one row per security            | `security_id`, `ticker`, `exchange`, `sector`, `listed_date`                               |
| `orders`            | one row per order source record | `order_id`, `account_id`, `customer_id`, `security_id`, `order_timestamp`, `order_status`  |
| `trades`            | one row per executed trade      | `trade_id`, `order_id`, `security_id`, `trade_timestamp`, `trade_price`, `trade_quantity`  |
| `cash_transactions` | one row per cash movement       | `cash_transaction_id`, `account_id`, `transaction_timestamp`, `transaction_type`, `amount` |

### 2.2 Offline Data Problems

Compulsory issues implemented:

- `Skew`: around 80% of order and event activity is concentrated on the top 20% of securities.
- `High cardinality`: `customer_id`, `account_id`, `order_id`, `trade_id`, and `cash_transaction_id` are mostly unique.
- `Schema evolution`: historical `orders` rows before `schema_change_date` have missing `order_channel`.

Optional issue implemented:

- `Duplicates`: about 2% duplicate order rows are injected using the same `order_id` with a later `created_ts`.

Downstream deduplication expectation:

- deduplicate `orders` by `order_id`
- keep the latest record by `created_ts` or `updated_ts`

### 2.3 Offline Output Format

- Parquet files
- easy to inspect locally before ingestion

---

## 3. Streaming Dataset Design

Streaming source files are written under:

```text
data/stream/trading_events/events.jsonl
```

The project can also send these events through Kafka before they are consumed back into JSONL for Bronze ingestion.

### 3.1 Event Stream Schema

Single event stream with an `event_type` column.

Key columns:

- `event_id`
- `event_type`
- `event_timestamp`
- `created_ts`
- `customer_id`
- `account_id`
- `security_id`
- `ticker`
- `session_id`
- `device_type`
- `source`

### 3.2 Event Types

- `ORDER_PLACED`
- `ORDER_CANCELLED`
- `ORDER_REJECTED`
- `ORDER_MATCHED`
- `PRICE_VIEWED`
- `WATCHLIST_ADDED`
- `LOGIN`
- `LOGOUT`

### 3.3 Streaming Data Problems

Compulsory issues implemented:

- `Bursts`: traffic increases strongly at market open and market close.
- `Late arrivals`: about 12% of events have `created_ts` later than `event_timestamp`.

Optional issue implemented:

- `Duplicates`: about 1.5% duplicate events using the same `event_id`.

Burst windows:

| Window       | Time    | Duration | Multiplier |
| ------------ | ------- | -------: | ---------: |
| Market open  | `09:00` |   15 min |        10x |
| Market close | `14:45` |   15 min |        10x |

### 3.4 Streaming Output Format

- JSON Lines for raw file inspection
- optional Kafka transport in the full pipeline

---

## 4. Feature Engineering Inputs

The generated data supports both offline and streaming feature pipelines.

Offline feature examples:

- `f_customer_total_orders_90d`
- `f_customer_avg_order_amount_90d`
- `f_customer_distinct_tickers_90d`
- `f_customer_total_trade_amount_90d`
- `f_customer_total_fee_amount_90d`

Streaming feature examples:

- `f_stream_event_count_60m`
- `f_stream_order_placed_count_60m`
- `f_stream_order_matched_count_60m`
- `f_stream_price_viewed_count_60m`
- `f_stream_distinct_ticker_count_60m`
- `f_stream_late_event_count_60m`

Unified feature idea:

- merge stable 90-day customer features with recent 60-minute stream behaviour

---

## 5. Generator Configuration

The generator is configured through `config.yaml`.

Representative parameters:

```yaml
n_customers: 50000
n_accounts: 60000
n_securities: 500
n_orders: 200000
days_history: 180
top_stock_skew_ratio: 0.80
duplicate_rate_orders: 0.02
schema_change_date: "2026-05-19"
base_events_per_min: 200
market_open_burst_multiplier: 10
market_close_burst_multiplier: 10
market_open_time: "09:00"
market_close_time: "14:45"
late_arrival_rate: 0.12
duplicate_rate_stream: 0.015
random_seed: 42
```

Design choices:

- deterministic seed for reproducibility
- configurable volume and skew
- configurable duplicate and late-arrival rates
- configurable schema change date

---

## 6. Deliverables

1. Generator code with configurable parameters
2. Raw data outputs:
   - Parquet for offline datasets
   - JSONL for streaming events
3. Evidence outputs:
   - data profile
   - skew distribution
   - duplicate summary
   - schema evolution report
   - stream late/duplicate/burst summary
4. Charts and samples for quick inspection

Important evidence files:

- `outputs/reports/data_profile.json`
- `outputs/reports/quality_report.json`
- `outputs/reports/duplicate_summary.csv`
- `outputs/reports/skew_summary.json`
- `outputs/reports/skew_distribution.csv`
- `outputs/reports/stream_summary.json`
- `outputs/reports/schema_evolution_report.json`
- `outputs/charts/skew_distribution.png`
- `outputs/charts/event_volume_by_minute.png`

---

## 7. Implementation Files

| File                                | Purpose                          |
| ----------------------------------- | -------------------------------- |
| `config.yaml`                       | generator parameters             |
| `src/common/config.py`              | shared configuration loader      |
| `src/common/paths.py`               | shared local and lakehouse paths |
| `src/generator/generate_offline.py` | offline Parquet generation       |
| `src/generator/generate_stream.py`  | stream JSONL generation          |
| `src/generator/generate_reports.py` | reports, samples, and charts     |

---

## 8. Run Instructions

Module-based run:

```bash
python -m src.generator.generate_offline
python -m src.generator.generate_stream
python -m src.generator.generate_reports
```

Or with Make:

```bash
make generate
make reports
```

---

## 9. Data Contracts for Downstream Pipelines

### 9.1 Offline Contract

- business keys: `customer_id`, `account_id`, `security_id`, `order_id`, `trade_id`, `cash_transaction_id`
- important timestamps: `order_timestamp`, `trade_timestamp`, `transaction_timestamp`
- technical metadata: `created_ts`, `updated_ts`
- duplicate policy: duplicates may exist in `orders`
- schema evolution policy: `order_channel` may be null in historical rows

### 9.2 Streaming Contract

- business key: `event_id`
- event-time column: `event_timestamp`
- arrival-time column: `created_ts`
- duplicate policy: duplicate `event_id` values may exist
- late-arrival policy: `created_ts` may be later than `event_timestamp`

---

## 10. Limitations

- prices and market behaviour are synthetic rather than market-realistic
- the generator does not simulate a full exchange matching engine
- streaming data is generated as JSONL first, then optionally transported through Kafka
- data issues are coursework-focused rather than production-complete
