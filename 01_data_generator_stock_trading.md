# 01 Data Generator - Stock Trading Platform

## 1. Objective

This section designs and implements a configurable data generator for a simulated stock trading platform. The generator produces both offline historical datasets and streaming trading events so that later sections can design ingestion, transformation, Gold-zone schemas, and AI/ML-ready features.

The generated data is intentionally not perfectly clean. It includes realistic data engineering challenges such as duplicate records, skewed trading activity, late-arriving stream events, burst traffic during market open/close, high-cardinality identifiers, and schema evolution.

## 2. Scope and Assumptions

### 2.1 In Scope

The generator covers a simplified securities trading domain with the following entities:

- Customers
- Trading accounts
- Securities
- Orders
- Trades
- Cash transactions
- Real-time trading events

The generated datasets are designed to support downstream analytics and feature engineering, including customer activity, order behavior, trading volume, market activity, and streaming event monitoring.

### 2.2 Out of Scope

The current generator does not simulate a full matching engine, real exchange connectivity, settlement lifecycle, or regulatory reporting. Prices are synthetic and are not connected to real market prices.

### 2.3 Main Assumptions

- One customer may own one or more trading accounts.
- One account may create many orders.
- Filled or partially filled orders may produce one or more trades.
- Cash transactions can be generated from trades or from extra account movements such as deposits, withdrawals, fees, and dividends.
- Streaming events represent user and trading platform activity during one trading day.
- `event_timestamp` represents when the event happened.
- `created_ts` represents when the record was created or ingested by the platform.

## 3. Generator Configuration

The generator is controlled through `config.yaml`.

```yaml
# Offline
n_customers: 50000
n_accounts: 60000
n_securities: 500
n_orders: 200000
days_history: 180
top_stock_skew_ratio: 0.80
duplicate_rate_orders: 0.02

# Stream
duplicate_rate_stream: 0.015
late_arrival_rate: 0.12
base_events_per_min: 200
market_open_burst_multiplier: 10
market_close_burst_multiplier: 10
market_open_time: "09:00"
market_open_duration_min: 15
market_close_time: "14:45"
market_close_duration_min: 15

# Others
schema_change_date: "2026-05-19"
random_seed: 42
```

> Note: `schema_change_date` must be inside the generated 180-day window. The generated orders currently cover approximately `2025-12-05` to `2026-06-03`, so `2026-05-19` is used to create both old-schema and new-schema records.

## 4. Offline Dataset Design

Offline datasets are generated as Parquet files under:

```text
data/raw/offline/
```

### 4.1 Offline Tables

| Table | Grain | Primary / Business Key | Main Timestamp Columns | Description |
|---|---|---|---|---|
| `customers` | One row per customer | `customer_id` | `signup_ts`, `created_ts`, `updated_ts` | Customer demographic and risk profile data |
| `accounts` | One row per account | `account_id` | `opened_ts`, `closed_ts`, `created_ts`, `updated_ts` | Trading account data linked to customers |
| `securities` | One row per security | `security_id`, `ticker` | `listed_date`, `created_ts`, `updated_ts` | Tradable instruments such as stocks, ETFs, bonds, and derivatives |
| `orders` | One row per order source record | `order_id` | `order_timestamp`, `created_ts`, `updated_ts` | Trading orders with intentional duplicate records |
| `trades` | One row per trade fill | `trade_id` | `trade_timestamp`, `created_ts` | Executed trades generated from filled or partially filled orders |
| `cash_transactions` | One row per cash movement | `cash_transaction_id` | `transaction_timestamp`, `created_ts` | Deposits, withdrawals, fees, dividends, and trade-related cash movements |

### 4.2 Main Order Schema

The `orders` table is the main offline fact-like source for Section 01.

Important columns:

| Column | Description |
|---|---|
| `order_id` | Business identifier of the order |
| `account_id` | Trading account that placed the order |
| `customer_id` | Customer owning the account |
| `security_id` | Security being traded |
| `ticker` | Human-readable security code |
| `order_side` | BUY or SELL |
| `order_type` | LIMIT or MARKET |
| `order_status` | NEW, PARTIALLY_FILLED, FILLED, CANCELLED, or REJECTED |
| `order_quantity` | Requested order quantity |
| `limit_price` | Synthetic order price |
| `order_channel` | New schema column added after the schema change date |
| `order_timestamp` | Business event time of the order |
| `created_ts` | Record creation time |
| `updated_ts` | Last update time |

## 5. Streaming Dataset Design

Streaming data is generated as JSON Lines under:

```text
data/raw/stream/trading_events/events.jsonl
```

### 5.1 Stream Event Grain

The stream dataset uses one row per trading platform event.

### 5.2 Stream Event Schema

| Column | Description |
|---|---|
| `event_id` | Unique event identifier, except intentionally duplicated events |
| `account_id` | Account linked to the event |
| `customer_id` | Customer linked to the event |
| `security_id` | Security linked to the event |
| `ticker` | Security ticker |
| `event_type` | Type of event, such as `ORDER_PLACED`, `ORDER_MATCHED`, `PRICE_VIEWED`, or `LOGIN` |
| `event_timestamp` | Time when the event actually happened |
| `created_ts` | Time when the event arrived or was created in the system |
| `session_id` | Synthetic user session identifier |
| `device_type` | IOS, ANDROID, or WEB |
| `source` | APP or WEB |

### 5.3 Stream Event Types

The generator currently supports:

- `ORDER_PLACED`
- `ORDER_CANCELLED`
- `ORDER_REJECTED`
- `ORDER_MATCHED`
- `PRICE_VIEWED`
- `WATCHLIST_ADDED`
- `LOGIN`
- `LOGOUT`

## 6. Injected Data Challenges

### 6.1 Duplicate Offline Orders

The generator injects duplicate order records by copying existing orders with the same `order_id` and a slightly later `created_ts`. This simulates duplicate source ingestion or repeated upstream delivery.

Evidence from `duplicate_summary.csv`:

| Metric | Value |
|---|---:|
| Total rows | 204,000 |
| Unique orders | 200,000 |
| Extra duplicate rows | 4,000 |
| Duplicated rows including original | 8,000 |
| Duplicate rate vs original | 2.0% |

Deduplication strategy for downstream sections:

- Use `order_id` as the business key.
- Keep the latest record by `created_ts` or `updated_ts` depending on the downstream table requirement.

### 6.2 Skewed Trading Activity

The generator simulates stock trading skew by sending around 80% of order activity to the top 20% of securities.

Evidence from `skew_summary.json`:

| Metric | Value |
|---|---:|
| Total orders | 204,000 |
| Unique securities in orders | 494 |
| Top 20% securities | 98 |
| Orders in top 20% securities | 160,220 |
| Actual top 20% share | 78.54% |
| Expected skew ratio | 80.00% |

This creates a realistic high-skew workload for joins and aggregations in later pipelines.

### 6.3 High-Cardinality Identifiers

The generated data contains many high-cardinality identifiers:

| Identifier | Purpose |
|---|---|
| `customer_id` | Customer-level analytics and features |
| `account_id` | Account-level joins and ownership analysis |
| `order_id` | Order-level deduplication and fact modeling |
| `trade_id` | Trade-level transaction modeling |
| `event_id` | Stream event deduplication |
| `session_id` | User activity and session-level analysis |

Evidence from the generated profile:

| Metric | Value |
|---|---:|
| Order rows | 204,000 |
| Unique orders | 200,000 |
| Unique customers in orders | 34,260 |
| Unique securities in orders | 494 |

These identifiers are important because downstream pipelines must handle large joins, distinct counts, and deduplication by business keys.

### 6.4 Schema Evolution

Schema evolution is simulated by introducing a new column named `order_channel` after the schema change date.

- Before `2026-05-19`, historical orders have `order_channel = null`.
- After `2026-05-19`, new orders contain values such as `APP`, `WEB`, or `BROKER`.

Evidence from `schema_evolution_report.json`:

| Metric | Value |
|---|---:|
| Schema change date | 2026-05-19 |
| New column | `order_channel` |
| Rows before change | 185,892 |
| Rows after change | 18,108 |
| Null `order_channel` before change | 185,892 |
| Null `order_channel` after change | 0 |
| Non-null `order_channel` after change | 18,108 |

This demonstrates that downstream pipelines must support old records that do not contain newly introduced fields.

### 6.5 Streaming Duplicate Events

The stream generator duplicates 1.5% of generated events by copying existing events with the same `event_id` and a delayed `created_ts`.

Evidence from `stream_summary.json`:

| Metric | Value |
|---|---:|
| Total stream events | 127,890 |
| Unique events | 126,000 |
| Extra duplicate events | 1,890 |
| Duplicate event rate vs original | 1.5% |
| Expected duplicate rate | 1.5% |

Downstream deduplication strategy:

- Use `event_id` as the stream event business key.
- Keep the latest event record by `created_ts` when duplicates exist.

### 6.6 Late-Arriving Stream Events

Late-arriving events are generated by making `event_timestamp` earlier than `created_ts`. This simulates events that happened in the past but arrived late to the ingestion system.

Evidence from `stream_summary.json`:

| Metric | Value |
|---|---:|
| Late events | 16,946 |
| Late event rate | 13.25% |
| Expected late arrival rate | 12.00% |
| Average late delay | 1,672.64 seconds |
| Maximum late delay | 3,720 seconds |

Downstream strategy:

- Use event-time processing based on `event_timestamp`.
- Use `created_ts` for arrival-time tracking and deduplication.
- Reprocess affected time windows when late events arrive.

### 6.7 Bursty Stream Traffic

The stream generator simulates market open and market close bursts.

Configuration:

| Window | Start Time | Duration | Multiplier |
|---|---:|---:|---:|
| Market open | 09:00 | 15 minutes | 10x |
| Market close | 14:45 | 15 minutes | 10x |

Evidence from `stream_summary.json` and `event_volume_by_minute.csv`:

| Metric | Value |
|---|---:|
| Base events per minute | 200 |
| Maximum events per minute | 2,035 |
| Average events per minute | 352.31 |

This simulates real stock-market behavior where trading systems experience high traffic at market open and market close.

## 7. Output Files and Evidence

### 7.1 Generated Raw Data

| Path | Description |
|---|---|
| `data/raw/offline/customers.parquet` | Customer master data |
| `data/raw/offline/accounts.parquet` | Account master data |
| `data/raw/offline/securities.parquet` | Security master data |
| `data/raw/offline/orders.parquet` | Generated order source data |
| `data/raw/offline/trades.parquet` | Generated trade executions |
| `data/raw/offline/cash_transactions.parquet` | Generated cash movements |
| `data/raw/stream/trading_events/events.jsonl` | Generated stream events |

### 7.2 Reports

| Report | Purpose |
|---|---|
| `outputs/reports/data_profile.json` | Basic row count, columns, timestamp range, and customer count |
| `outputs/reports/quality_report.json` | Basic quality summary for orders |
| `outputs/reports/duplicate_summary.csv` | Offline duplicate summary |
| `outputs/reports/skew_summary.json` | Top-security skew summary |
| `outputs/reports/skew_distribution.csv` | Per-ticker order count distribution |
| `outputs/reports/stream_summary.json` | Streaming duplicate, late arrival, and burst summary |
| `outputs/reports/event_volume_by_minute.csv` | Per-minute stream event volume |
| `outputs/reports/schema_evolution_report.json` | Schema evolution evidence for `order_channel` |

### 7.3 Samples and Charts

| Output | Purpose |
|---|---|
| `outputs/samples/sample_orders.parquet` | Sample offline order records |
| `outputs/samples/sample_trading_events.jsonl` | Sample stream events |
| `outputs/charts/skew_distribution.png` | Visualization of top securities by order count |
| `outputs/charts/event_volume_by_minute.png` | Visualization of stream traffic by minute |

## 8. Implementation Files

| File | Purpose |
|---|---|
| `config.yaml` | Central generator configuration |
| `config.py` | Helper function for loading YAML configuration |
| `generate_offline.py` | Generates offline Parquet datasets |
| `generate_stream.py` | Generates JSONL stream events |
| `generate_reports.py` | Generates reports, samples, and charts |

## 9. Run Instructions

### 9.1 Local Run

Install dependencies first:

```bash
pip install pandas numpy faker pyyaml matplotlib pyarrow
```

Generate offline data:

```bash
python generate_offline.py
```

Generate streaming data:

```bash
python generate_stream.py
```

Generate reports, samples, and charts:

```bash
python generate_reports.py
```

### 9.2 Planned Docker Compose Run

Docker Compose support will be added after the Section 02 pipeline services are finalized. The expected workflow is:

```bash
docker compose up data-generator
docker compose up report-generator
```

This will improve reproducibility by making the generator runnable with a fixed environment and consistent dependency versions.

## 10. Data Contracts for Downstream Sections

### 10.1 Offline Contract

Downstream pipelines should treat the offline data as raw Bronze input.

Minimum contract:

- Required business keys: `customer_id`, `account_id`, `security_id`, `order_id`, `trade_id`, `cash_transaction_id`
- Required timestamps: `order_timestamp`, `trade_timestamp`, `transaction_timestamp`, `created_ts`, `updated_ts`
- Duplicate policy: duplicates may exist in `orders`; downstream Silver tables must deduplicate by `order_id`
- Schema evolution policy: `order_channel` may be null for historical records before `2026-05-19`

### 10.2 Streaming Contract

Downstream streaming pipelines should treat `events.jsonl` as raw event input.

Minimum contract:

- Required event key: `event_id`
- Event-time column: `event_timestamp`
- Arrival-time column: `created_ts`
- Duplicate policy: duplicate `event_id` values may exist and must be deduplicated
- Late-arrival policy: `event_timestamp` may be earlier than `created_ts`, so event-time windows must support late data

## 11. Monitoring and Quality Checks

The following checks are expected in downstream Section 02 pipelines:

| Check | Target |
|---|---|
| Order uniqueness | `order_id` unique after Silver deduplication |
| Stream uniqueness | `event_id` unique after Silver deduplication |
| Referential integrity | Orders must link to valid accounts and securities |
| Null checks | Required keys and timestamps must not be null |
| Schema checks | New columns should not break old partitions |
| Volume checks | Alert if row counts are unexpectedly low or high |
| Freshness checks | Alert if generated or ingested data is stale |
| Burst monitoring | Track event-per-minute spikes during market open and close |
| Late-arrival monitoring | Track late event rate and maximum delay |

## 12. Limitations and Next Steps

Current limitations:

- Synthetic prices are not based on real market movements.
- The generator does not simulate a real exchange matching engine.
- Stream generation currently simulates one trading day rather than continuous multi-day streaming.
- Docker Compose support is planned but not finalized in this section.

Next steps for Section 02:

- Design Bronze, Silver, and Gold storage layers.
- Define deduplication rules for orders and events.
- Define schema evolution handling for `order_channel`.
- Create dimension and fact tables for trading analytics.
- Define feature tables for downstream ML or AI use cases.
- Add pipeline-level monitoring, recovery, and backfill policy.
