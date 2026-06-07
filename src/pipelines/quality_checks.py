import json
import uuid
from datetime import datetime, timezone

from pyspark.sql import functions as F

from src.common.paths import REPORTS_DIR, RUN_LOGS_DIR
from src.common.spark import get_spark_session, layer_path


REPORTS_DIR.mkdir(parents=True, exist_ok=True)
RUN_LOGS_DIR.mkdir(parents=True, exist_ok=True)


def now_utc():
    return datetime.now(timezone.utc).isoformat()


def read_delta(spark, layer: str, table_name: str):
    return spark.read.format("delta").load(layer_path(layer, table_name))


def check_unique(df, table_name, column):
    total_rows = df.count()
    distinct_rows = df.select(column).distinct().count()
    duplicate_count = total_rows - distinct_rows
    return {
        "check_name": "unique_key",
        "table": table_name,
        "column": column,
        "status": "PASS" if duplicate_count == 0 else "FAIL",
        "duplicate_count": int(duplicate_count),
    }


def check_not_null(df, table_name, columns):
    results = []
    row = df.select([F.sum(F.col(column).isNull().cast("int")).alias(column) for column in columns]).collect()[0].asDict()
    for column in columns:
        null_count = row[column]
        results.append(
            {
                "check_name": "not_null",
                "table": table_name,
                "column": column,
                "status": "PASS" if null_count == 0 else "FAIL",
                "null_count": int(null_count),
            }
        )
    return results


def check_referential_integrity(fact_df, dim_df, fact_table, dim_table, fact_key, dim_key):
    missing_count = fact_df.join(dim_df.select(dim_key).distinct(), fact_df[fact_key] == dim_df[dim_key], "left_anti").count()
    return {
        "check_name": "referential_integrity",
        "fact_table": fact_table,
        "dim_table": dim_table,
        "fact_key": fact_key,
        "dim_key": dim_key,
        "status": "PASS" if missing_count == 0 else "FAIL",
        "missing_count": int(missing_count),
    }


def check_volume(table_name, df, min_rows=1):
    row_count = df.count()
    return {
        "check_name": "volume_min_rows",
        "table": table_name,
        "status": "PASS" if row_count >= min_rows else "FAIL",
        "row_count": int(row_count),
        "min_rows": int(min_rows),
    }


def check_freshness(table_name, df, timestamp_col):
    max_ts = df.select(F.max(timestamp_col).alias("max_ts")).collect()[0]["max_ts"]
    return {
        "check_name": "freshness_available",
        "table": table_name,
        "timestamp_col": timestamp_col,
        "status": "PASS" if max_ts is not None else "FAIL",
        "max_timestamp": None if max_ts is None else str(max_ts),
    }


def run_quality_checks():
    spark = get_spark_session("quality-checks")
    run_id = str(uuid.uuid4())
    start_ts = now_utc()
    checks = []

    silver_orders = read_delta(spark, "silver", "stg_orders")
    silver_events = read_delta(spark, "silver", "stg_trading_events")
    dim_customer = read_delta(spark, "gold", "dim_customer")
    dim_account = read_delta(spark, "gold", "dim_account")
    dim_security = read_delta(spark, "gold", "dim_security")
    fact_order = read_delta(spark, "gold", "fact_order")
    fact_trade = read_delta(spark, "gold", "fact_trade")
    feat_customer_90d = read_delta(spark, "gold", "feat_customer_90d")
    feat_stream_customer_60m = read_delta(spark, "gold", "feat_stream_customer_60m")

    checks.extend([
        check_volume("stg_orders", silver_orders),
        check_volume("stg_trading_events", silver_events),
        check_volume("fact_order", fact_order),
        check_volume("fact_trade", fact_trade),
        check_volume("feat_customer_90d", feat_customer_90d),
        check_volume("feat_stream_customer_60m", feat_stream_customer_60m),
        check_unique(silver_orders, "stg_orders", "order_id"),
        check_unique(silver_events, "stg_trading_events", "event_id"),
        check_unique(dim_customer, "dim_customer", "customer_id"),
        check_unique(dim_account, "dim_account", "account_id"),
        check_unique(dim_security, "dim_security", "security_id"),
        check_unique(fact_order, "fact_order", "order_id"),
        check_unique(fact_trade, "fact_trade", "trade_id"),
    ])

    checks.extend(check_not_null(fact_order, "fact_order", ["order_id", "customer_id", "account_id", "security_id", "order_timestamp", "order_amount"]))
    checks.extend(check_not_null(fact_trade, "fact_trade", ["trade_id", "order_id", "customer_id", "account_id", "security_id", "trade_timestamp", "trade_amount"]))

    checks.extend([
        check_referential_integrity(fact_order, dim_customer, "fact_order", "dim_customer", "customer_id", "customer_id"),
        check_referential_integrity(fact_order, dim_account, "fact_order", "dim_account", "account_id", "account_id"),
        check_referential_integrity(fact_order, dim_security, "fact_order", "dim_security", "security_id", "security_id"),
        check_freshness("fact_order", fact_order, "order_timestamp"),
        check_freshness("fact_trade", fact_trade, "trade_timestamp"),
        check_freshness("stg_trading_events", silver_events, "event_timestamp"),
    ])

    failed_checks = [check for check in checks if check["status"] == "FAIL"]
    quality_report = {
        "run_id": run_id,
        "start_ts": start_ts,
        "end_ts": now_utc(),
        "status": "PASS" if not failed_checks else "FAIL",
        "total_checks": len(checks),
        "failed_checks": len(failed_checks),
        "checks": checks,
    }

    report_path = REPORTS_DIR / "pipeline_quality_report.json"
    report_path.write_text(json.dumps(quality_report, indent=2))

    run_log = {
        "run_id": run_id,
        "pipeline_name": "quality_checks",
        "start_ts": start_ts,
        "end_ts": quality_report["end_ts"],
        "status": quality_report["status"],
        "input_tables": [
            "stg_orders",
            "stg_trading_events",
            "dim_customer",
            "dim_account",
            "dim_security",
            "fact_order",
            "fact_trade",
            "feat_customer_90d",
            "feat_stream_customer_60m",
        ],
        "output_files": [str(report_path)],
        "error_summary": None if not failed_checks else failed_checks[:5],
    }

    run_log_path = RUN_LOGS_DIR / f"quality_checks_{run_id}.json"
    run_log_path.write_text(json.dumps(run_log, indent=2))

    print("Quality checks completed")
    print(f"status={quality_report['status']}")
    print(f"report={report_path}")
    print(f"run_log={run_log_path}")

    spark.stop()


if __name__ == "__main__":
    run_quality_checks()
