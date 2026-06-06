import uuid
from datetime import datetime, timezone
from pathlib import Path
import sys

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[2]))

from paths import (
    OFFLINE_DIR,
    PROJECT_DIR,
    STREAM_EVENTS_PATH,
)

BRONZE_DIR = PROJECT_DIR / "data" / "bronze"


OFFLINE_TABLES = [
    "customers",
    "accounts",
    "securities",
    "orders",
    "trades",
    "cash_transactions",
]


def now_utc():
    return datetime.now(timezone.utc).isoformat()


def add_ingest_metadata(df: pd.DataFrame, source_file: str, batch_id: str):
    df = df.copy()
    df["ingest_ts"] = now_utc()
    df["batch_id"] = batch_id
    df["source_file"] = source_file
    return df


def ingest_offline_table(table_name: str, batch_id: str):
    source_path = OFFLINE_DIR / f"{table_name}.parquet"

    if not source_path.exists():
        raise FileNotFoundError(f"Missing source file: {source_path}")

    df = pd.read_parquet(source_path)
    df = add_ingest_metadata(
        df=df,
        source_file=str(source_path),
        batch_id=batch_id,
    )

    output_dir = BRONZE_DIR / table_name
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / f"batch_id={batch_id}.parquet"
    df.to_parquet(output_path, index=False)

    return {
        "table": table_name,
        "input_path": str(source_path),
        "output_path": str(output_path),
        "rows": len(df),
    }


def ingest_stream_events(batch_id: str):
    source_path = STREAM_EVENTS_PATH

    if not source_path.exists():
        raise FileNotFoundError(f"Missing stream file: {source_path}")

    df = pd.read_json(source_path, lines=True)
    df = add_ingest_metadata(
        df=df,
        source_file=str(source_path),
        batch_id=batch_id,
    )

    output_dir = BRONZE_DIR / "trading_events"
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / f"batch_id={batch_id}.parquet"
    df.to_parquet(output_path, index=False)

    return {
        "table": "trading_events",
        "input_path": str(source_path),
        "output_path": str(output_path),
        "rows": len(df),
    }


def run_bronze_ingestion():
    batch_id = str(uuid.uuid4())

    results = []

    for table in OFFLINE_TABLES:
        result = ingest_offline_table(table, batch_id)
        results.append(result)

    results.append(ingest_stream_events(batch_id))

    print("Bronze ingestion completed")
    print(f"batch_id={batch_id}")

    for result in results:
        print(
            f"[{result['table']}] rows={result['rows']} output={result['output_path']}"
        )


if __name__ == "__main__":
    run_bronze_ingestion()
