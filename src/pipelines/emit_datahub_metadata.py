from __future__ import annotations

import os

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DataHubRestEmitter
from datahub.metadata.schema_classes import (
    DataFlowInfoClass,
    DataJobInfoClass,
    DataJobInputOutputClass,
    StatusClass,
)

PROJECT_NAME = "stock_trading_pipeline"
ENVIRONMENT = "PROD"
PLATFORM = "airflow"


def dataflow_urn() -> str:
    return f"urn:li:dataFlow:({PLATFORM},{PROJECT_NAME},{ENVIRONMENT})"


def datajob_urn(task_id: str) -> str:
    return f"urn:li:dataJob:({dataflow_urn()},{task_id})"


def dataset_urn(platform: str, name: str, env: str = "PROD") -> str:
    return f"urn:li:dataset:(urn:li:dataPlatform:{platform},{name},{env})"


LINEAGE = {
    "generate_offline": {
        "inputs": [],
        "outputs": [
            dataset_urn("file", "stock_trading/raw/customers"),
            dataset_urn("file", "stock_trading/raw/accounts"),
            dataset_urn("file", "stock_trading/raw/securities"),
            dataset_urn("file", "stock_trading/raw/orders"),
            dataset_urn("file", "stock_trading/raw/trades"),
            dataset_urn("file", "stock_trading/raw/cash_transactions"),
        ],
    },
    "generate_stream": {
        "inputs": [],
        "outputs": [
            dataset_urn("file", "stock_trading/raw/trading_events"),
        ],
    },
    "bronze_ingestion": {
        "inputs": [
            dataset_urn("file", "stock_trading/raw/orders"),
            dataset_urn("file", "stock_trading/raw/trading_events"),
        ],
        "outputs": [
            dataset_urn("delta", "stock_lakehouse.bronze_orders"),
            dataset_urn("delta", "stock_lakehouse.bronze_trading_events"),
        ],
    },
    "silver_transform": {
        "inputs": [
            dataset_urn("delta", "stock_lakehouse.bronze_orders"),
            dataset_urn("delta", "stock_lakehouse.bronze_trading_events"),
        ],
        "outputs": [
            dataset_urn("delta", "stock_lakehouse.silver_orders"),
            dataset_urn("delta", "stock_lakehouse.silver_trading_events"),
        ],
    },
    "gold_transform": {
        "inputs": [
            dataset_urn("delta", "stock_lakehouse.silver_orders"),
            dataset_urn("delta", "stock_lakehouse.silver_trading_events"),
        ],
        "outputs": [
            dataset_urn("delta", "stock_lakehouse.gold_fact_orders"),
            dataset_urn("delta", "stock_lakehouse.gold_fact_trades"),
            dataset_urn("delta", "stock_lakehouse.gold_obt_order_performance"),
        ],
    },
    "feature_transform": {
        "inputs": [
            dataset_urn("delta", "stock_lakehouse.gold_fact_orders"),
            dataset_urn("delta", "stock_lakehouse.silver_trading_events"),
        ],
        "outputs": [
            dataset_urn("delta", "stock_lakehouse.feat_customer_90d"),
            dataset_urn("delta", "stock_lakehouse.feat_stream_60m"),
            dataset_urn("delta", "stock_lakehouse.feat_customer_unified"),
        ],
    },
    "export_postgres": {
        "inputs": [
            dataset_urn("delta", "stock_lakehouse.gold_fact_orders"),
            dataset_urn("delta", "stock_lakehouse.gold_obt_order_performance"),
            dataset_urn("delta", "stock_lakehouse.feat_customer_unified"),
        ],
        "outputs": [
            dataset_urn("postgres", "stock_dw.gold_fact_orders"),
            dataset_urn("postgres", "stock_dw.gold_obt_order_performance"),
            dataset_urn("postgres", "stock_dw.feat_customer_unified"),
        ],
    },
}


def emit_mcp(emitter: DataHubRestEmitter, urn: str, aspect) -> None:
    emitter.emit_mcp(
        MetadataChangeProposalWrapper(
            entityUrn=urn,
            aspect=aspect,
        )
    )


def main() -> None:
    if os.environ.get("AIRFLOW__DATAHUB__ENABLED", "").lower() != "true":
        print("[emit_datahub_metadata] DataHub disabled; skipping")
        return

    gms_url = os.environ.get("DATAHUB_GMS_URL", "http://datahub-gms:8080")
    emitter = DataHubRestEmitter(gms_server=gms_url)

    try:
        flow_urn = dataflow_urn()

        emit_mcp(
            emitter,
            flow_urn,
            DataFlowInfoClass(
                name=PROJECT_NAME,
                description="Stock trading coursework Airflow pipeline",
                project="stock-trading-coursework",
            ),
        )
        emit_mcp(emitter, flow_urn, StatusClass(removed=False))

        for task_id, io in LINEAGE.items():
            job_urn = datajob_urn(task_id)

            emit_mcp(
                emitter,
                job_urn,
                DataJobInfoClass(
                    name=task_id,
                    type="BATCH",
                    description=f"Airflow task {task_id}",
                    flowUrn=flow_urn,
                ),
            )

            emit_mcp(
                emitter,
                job_urn,
                DataJobInputOutputClass(
                    inputDatasets=io["inputs"],
                    outputDatasets=io["outputs"],
                ),
            )

            emit_mcp(emitter, job_urn, StatusClass(removed=False))

        emitter.flush()
        print("[emit_datahub_metadata] Emitted DataHub flow, jobs, and lineage")

    finally:
        emitter.close()


if __name__ == "__main__":
    main()
