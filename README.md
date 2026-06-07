# Stock Trading Lakehouse Coursework

## Overview

This project is an end-to-end stock-trading data engineering coursework built around a small local lakehouse stack.

It generates synthetic stock-trading data, transports stream events through Kafka, processes Bronze/Silver/Gold layers with Spark, stores lakehouse tables in Delta Lake on MinIO, runs orchestration in Airflow, and exports curated outputs to PostgreSQL.

Core stack:

- Airflow for orchestration
- Spark for batch and stream-style processing
- Delta Lake for Bronze, Silver, and Gold storage format
- MinIO for S3-compatible object storage
- Kafka for event transport
- PostgreSQL for exported serving tables
- optional DataHub integration for lineage

Current scope note:

- the implemented feature store is offline and analytics-oriented
- online feature serving is planned as future work

## What This Coursework Demonstrates

- configurable offline and streaming data generation
- intentionally injected data quality problems
- Bronze, Silver, and Gold pipeline layering
- feature table computation
- orchestration with Airflow
- quality monitoring and run logs
- optional lineage metadata with DataHub

## Repository Structure

```text
src/common/          shared config, storage, Spark, and table helpers
src/generator/       offline generator, stream generator, reporting
src/pipelines/       Bronze, Silver, Gold, features, quality, export
airflow/dags/        orchestration DAG
airflow/plugins/     DataHub Airflow plugin code
data/                local raw source files
outputs/             reports, charts, samples, run logs
01_data_generator.md
02_schema_design.md
docker-compose.yml
data-hub-docker-compose.yaml
```

## Architecture

Data flow:

1. generate offline Parquet and streaming JSONL source data locally
2. optionally publish stream events to Kafka and consume them back into JSONL
3. ingest raw files into Bronze Delta tables on MinIO
4. transform and deduplicate into Silver Delta tables
5. model Gold dimensions, facts, OBT, and features
6. run quality checks and produce evidence outputs
7. export curated Gold outputs to PostgreSQL

### Architecture Diagram

```mermaid
flowchart LR
    A[Offline Generator] --> B[Raw Offline Parquet]
    C[Stream Generator] --> D[Kafka Topic]
    D --> E[JSONL Consumer]

    B --> F[Bronze Delta on MinIO]
    E --> F

    F --> G[Silver Delta on MinIO]
    G --> H[Gold Delta on MinIO]
    G --> I[Streaming Features]
    H --> J[Offline Features]
    H --> K[PostgreSQL Export]

    L[Airflow DAG] --> A
    L --> C
    L --> D
    L --> E
    L --> F
    L --> G
    L --> H
    L --> I
    L --> J
    L --> K

    M[DataHub Optional] -. metadata and lineage .-> L
    M -. metadata and lineage .-> F
    M -. metadata and lineage .-> G
    M -. metadata and lineage .-> H
```

Storage layout:

- local raw/source data:
  - `data/raw/offline/`
  - `data/stream/trading_events/`
- local evidence outputs:
  - `outputs/reports/`
  - `outputs/charts/`
  - `outputs/samples/`
  - `outputs/run_logs/`
- lakehouse tables on MinIO:
  - `s3a://stock-lakehouse/bronze/`
  - `s3a://stock-lakehouse/silver/`
  - `s3a://stock-lakehouse/gold/`
  - `s3a://stock-lakehouse/checkpoints/`

## Prerequisites

Required:

- Docker
- Docker Compose

Optional for local module execution outside containers:

- Python 3.12+

## Quick Start

### 1. Create the shared Docker network

The compose file expects an external Docker network named `data-stack-network`.

```bash
make network-up
```

### 2. Build and start the stack

```bash
docker compose up -d --build
```

This starts:

- Airflow webserver on `http://localhost:58080`
- Airflow scheduler
- Airflow metadata PostgreSQL
- serving PostgreSQL on `localhost:5432`
- Kafka on `localhost:9092`
- MinIO API on `http://localhost:9000`
- MinIO console on `http://localhost:9001`

Default credentials:

- Airflow
  - username: `airflow`
  - password: `airflow`
- MinIO
  - username: `minioadmin`
  - password: `minioadmin`
- PostgreSQL serving database
  - database: `stock_dw`
  - username: `postgres`
  - password: `postgres`

### 3. Trigger the pipeline in Airflow

1. Open `http://localhost:58080`
2. Log in with `airflow / airflow`
3. Enable the DAG `stock_trading_pipeline`
4. Trigger a manual run

Actual task order:

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

## Manual Execution

You can also run the pipeline modules from the project root.

Generator and reports:

```bash
python -m src.generator.generate_offline
python -m src.generator.generate_stream
python -m src.generator.generate_reports
```

Pipeline stages:

```bash
python -m src.pipelines.bronze_ingest
python -m src.pipelines.silver_transform
python -m src.pipelines.gold_transform
python -m src.pipelines.feature_transform
python -m src.pipelines.quality_checks
DATABASE_URL=postgresql+psycopg2://postgres:postgres@postgres:5432/stock_dw python -m src.pipelines.export_postgres
```

Kafka utilities:

```bash
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 python -m src.pipelines.kafka_producer
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 KAFKA_MAX_MESSAGES=1000 python -m src.pipelines.kafka_consumer_to_jsonl
```

## Make Targets

Useful commands:

```bash
make network-up
make airflow-init
make airflow-up
make generate
make reports
make bronze
make silver
make gold
make features
make quality
make postgres
make kafka-up
make kafka-produce
make kafka-consume
make datahub-up
make datahub-down
```

## What To Inspect For Marking

### Airflow

- successful `stock_trading_pipeline` run in the Airflow UI
- task logs for each stage

Example screenshot:

![Successful Airflow pipeline run](./images/airflow.png)

### MinIO

Bucket:

- `stock-lakehouse`

Key areas to inspect:

- `bronze/`
- `silver/`
- `gold/`

Example screenshot:

![MinIO lakehouse layout](./images/minio.png)

### Local Evidence Outputs

Important outputs:

- `outputs/reports/data_profile.json`
- `outputs/reports/quality_report.json`
- `outputs/reports/duplicate_summary.csv`
- `outputs/reports/skew_summary.json`
- `outputs/reports/skew_distribution.csv`
- `outputs/reports/stream_summary.json`
- `outputs/reports/schema_evolution_report.json`
- `outputs/reports/pipeline_quality_report.json`
- `outputs/charts/skew_distribution.png`
- `outputs/charts/event_volume_by_minute.png`

### PostgreSQL Export

The pipeline exports curated tables such as:

- `dim_customer`
- `dim_account`
- `dim_security`
- `dim_date`
- `fact_order`
- `fact_trade`
- `fact_cash_transaction`
- `obt_customer_trading_activity`
- `feat_customer_90d`
- `feat_security_1d`
- `feat_stream_customer_60m`
- `feat_customer_unified`

Example screenshot:

![Exported PostgreSQL tables](./images/exported_postgres.png)

## DataHub Lineage

Optional lineage stack:

```bash
make datahub-up
```

This starts the overlay services from `data-hub-docker-compose.yaml`.

The DAG includes an `emit_datahub_metadata` task. If DataHub is not enabled, that task safely skips actual emission.

Current limitation:

- dataset metadata emission is implemented
- however, the Airflow pipeline itself is not yet shown correctly inside DataHub as a full workflow lineage view
- this is a remaining integration gap rather than a core pipeline failure

## Coursework Documents

- [01_data_generator.md](/Users/bovn/Downloads/EDAI-1/coursework/edai-1-mncw-stock-trading/01_data_generator.md)
- [02_schema_design.md](/Users/bovn/Downloads/EDAI-1/coursework/edai-1-mncw-stock-trading/02_schema_design.md)

## Notes and Limitations

- Bronze is append-only, so repeated reruns can increase Bronze row counts.
- Silver deduplicates records back to business-level rows.
- Gold and feature jobs are heavier than necessary for local resources, so Spark warnings can appear even when jobs succeed.
- The implementation is coursework-focused rather than production-optimised.
- The current feature store is an offline analytical store. An online feature store or low-latency feature serving layer is not implemented yet.
- DataHub integration is partial: metadata emission exists, but Airflow workflow visibility in DataHub is still incomplete.

## Shut Down

Stop the core stack:

```bash
docker compose down
```

Stop the DataHub overlay only:

```bash
make datahub-down
```
