# 01 Data Generator - Stock Trading Platform

## 1. Objective
Explain that this generator simulates a stock trading platform with offline and streaming data.

## 2. Domain Scope and Assumptions
Explain what is included and what is out of scope.

## 3. Generator Configuration
Show important config values:
- n_customers
- n_accounts
- n_securities
- n_orders
- duplicate_rate_orders
- duplicate_rate_stream
- late_arrival_rate
- burst multipliers
- schema_change_date

## 4. Offline Dataset Design
Describe each table:
- customers
- accounts
- securities
- orders
- trades
- cash_transactions

Include grain, primary key, timestamp columns.

## 5. Streaming Dataset Design
Describe trading_events:
- event_id
- event_type
- event_timestamp
- created_ts
- account_id
- customer_id
- security_id
- ticker
- session_id
- device_type
- source

## 6. Data Challenges Injected
Explain:
- duplicate orders
- skewed securities
- duplicate stream events
- late arrivals
- burst traffic
- schema evolution

## 7. Evidence Reports
Reference your generated files:
- data_profile.json
- quality_report.json
- duplicate_summary.csv
- skew_summary.json
- skew_distribution.csv
- stream_summary.json
- event_volume_by_minute.csv
- schema_evolution_report.json

## 8. Sample Outputs
Mention:
- sample_orders.parquet
- sample_trading_events.jsonl
- charts/skew_distribution.png
- charts/event_volume_by_minute.png

## 9. Run Instructions
Example:
python generate_offline.py
python generate_stream.py
python generate_reports.py

## 10. Limitations and Next Steps
Mention Section 02 will use this data for Bronze/Silver/Gold schema design.
