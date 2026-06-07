import uuid

from pyspark.sql import functions as F

from src.common.paths import OFFLINE_DIR, STREAM_EVENTS_PATH
from src.common.spark import add_ingest_metadata, get_spark_session, layer_path, write_delta
from src.common.tables import OFFLINE_TABLES


def ingest_offline_table(spark, table_name: str, batch_id: str):
    source_path = OFFLINE_DIR / f"{table_name}.parquet"
    if not source_path.exists():
        raise FileNotFoundError(f"Missing source file: {source_path}")

    df = (
        spark.read.parquet(str(source_path))
        .withColumn("_source_file", F.input_file_name())
        .transform(add_ingest_metadata)
        .withColumn("batch_id", F.lit(batch_id))
    )

    output_path = layer_path("bronze", table_name)
    write_delta(df, output_path, mode="append")

    return {
        "table": table_name,
        "input_path": str(source_path),
        "output_path": output_path,
        "rows": df.count(),
    }


def ingest_stream_events(spark, batch_id: str):
    source_path = STREAM_EVENTS_PATH
    if not source_path.exists():
        raise FileNotFoundError(f"Missing stream file: {source_path}")

    df = (
        spark.read.json(str(source_path))
        .withColumn("_source_file", F.input_file_name())
        .transform(add_ingest_metadata)
        .withColumn("batch_id", F.lit(batch_id))
    )

    output_path = layer_path("bronze", "trading_events")
    write_delta(df, output_path, mode="append")

    return {
        "table": "trading_events",
        "input_path": str(source_path),
        "output_path": output_path,
        "rows": df.count(),
    }


def run_bronze_ingestion():
    spark = get_spark_session("bronze-ingestion")
    batch_id = str(uuid.uuid4())
    results = []

    for table in OFFLINE_TABLES:
        results.append(ingest_offline_table(spark, table, batch_id))

    results.append(ingest_stream_events(spark, batch_id))

    print("Bronze ingestion completed")
    print(f"batch_id={batch_id}")
    for result in results:
        print(f"[{result['table']}] rows={result['rows']} output={result['output_path']}")

    spark.stop()


if __name__ == "__main__":
    run_bronze_ingestion()
