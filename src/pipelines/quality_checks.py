import json
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[2]))


from paths import OUTPUTS_DIR, PROJECT_DIR

SILVER_DIR = PROJECT_DIR / "data" / "silver"
GOLD_DIR = PROJECT_DIR / "data" / "gold"
REPORTS_DIR = OUTPUTS_DIR / "reports"
RUN_LOG_DIR = OUTPUTS_DIR / "run_logs"

REPORTS_DIR.mkdir(parents=True, exist_ok=True)
RUN_LOG_DIR.mkdir(parents=True, exist_ok=True)


def now_utc():
    return datetime.now(timezone.utc).isoformat()


def read_parquet(path):
    if not path.exists():
        raise FileNotFoundError(f"Missing table: {path}")

    return pd.read_parquet(path)


def check_unique(df, table_name, column):
    duplicate_count = len(df) - df[column].nunique()

    return {
        "check_name": "unique_key",
        "table": table_name,
        "column": column,
        "status": "PASS" if duplicate_count == 0 else "FAIL",
        "duplicate_count": int(duplicate_count),
    }


def check_not_null(df, table_name, columns):
    results = []

    for column in columns:
        null_count = df[column].isna().sum()

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


def check_referential_integrity(
    fact_df,
    dim_df,
    fact_table,
    dim_table,
    fact_key,
    dim_key,
):
    missing_count = fact_df[~fact_df[fact_key].isin(dim_df[dim_key])].shape[0]

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
    row_count = len(df)

    return {
        "check_name": "volume_min_rows",
        "table": table_name,
        "status": "PASS" if row_count >= min_rows else "FAIL",
        "row_count": int(row_count),
        "min_rows": int(min_rows),
    }


def check_freshness(table_name, df, timestamp_col):
    max_ts = pd.to_datetime(df[timestamp_col], errors="coerce").max()

    return {
        "check_name": "freshness_available",
        "table": table_name,
        "timestamp_col": timestamp_col,
        "status": "PASS" if pd.notna(max_ts) else "FAIL",
        "max_timestamp": None if pd.isna(max_ts) else str(max_ts),
    }


def run_quality_checks():
    run_id = str(uuid.uuid4())
    start_ts = now_utc()

    checks = []

    silver_orders = read_parquet(SILVER_DIR / "stg_orders.parquet")
    silver_events = read_parquet(SILVER_DIR / "stg_trading_events.parquet")

    dim_customer = read_parquet(GOLD_DIR / "dim_customer.parquet")
    dim_account = read_parquet(GOLD_DIR / "dim_account.parquet")
    dim_security = read_parquet(GOLD_DIR / "dim_security.parquet")
    fact_order = read_parquet(GOLD_DIR / "fact_order.parquet")
    fact_trade = read_parquet(GOLD_DIR / "fact_trade.parquet")
    feat_customer_90d = read_parquet(GOLD_DIR / "feat_customer_90d.parquet")
    feat_stream_customer_60m = read_parquet(
        GOLD_DIR / "feat_stream_customer_60m.parquet"
    )

    checks.append(check_volume("stg_orders", silver_orders))
    checks.append(check_volume("stg_trading_events", silver_events))
    checks.append(check_volume("fact_order", fact_order))
    checks.append(check_volume("fact_trade", fact_trade))
    checks.append(check_volume("feat_customer_90d", feat_customer_90d))
    checks.append(check_volume("feat_stream_customer_60m", feat_stream_customer_60m))

    checks.append(check_unique(silver_orders, "stg_orders", "order_id"))
    checks.append(check_unique(silver_events, "stg_trading_events", "event_id"))
    checks.append(check_unique(dim_customer, "dim_customer", "customer_id"))
    checks.append(check_unique(dim_account, "dim_account", "account_id"))
    checks.append(check_unique(dim_security, "dim_security", "security_id"))
    checks.append(check_unique(fact_order, "fact_order", "order_id"))
    checks.append(check_unique(fact_trade, "fact_trade", "trade_id"))

    checks.extend(
        check_not_null(
            fact_order,
            "fact_order",
            [
                "order_id",
                "customer_id",
                "account_id",
                "security_id",
                "order_timestamp",
                "order_amount",
            ],
        )
    )

    checks.extend(
        check_not_null(
            fact_trade,
            "fact_trade",
            [
                "trade_id",
                "order_id",
                "customer_id",
                "account_id",
                "security_id",
                "trade_timestamp",
                "trade_amount",
            ],
        )
    )

    checks.append(
        check_referential_integrity(
            fact_df=fact_order,
            dim_df=dim_customer,
            fact_table="fact_order",
            dim_table="dim_customer",
            fact_key="customer_id",
            dim_key="customer_id",
        )
    )

    checks.append(
        check_referential_integrity(
            fact_df=fact_order,
            dim_df=dim_account,
            fact_table="fact_order",
            dim_table="dim_account",
            fact_key="account_id",
            dim_key="account_id",
        )
    )

    checks.append(
        check_referential_integrity(
            fact_df=fact_order,
            dim_df=dim_security,
            fact_table="fact_order",
            dim_table="dim_security",
            fact_key="security_id",
            dim_key="security_id",
        )
    )

    checks.append(check_freshness("fact_order", fact_order, "order_timestamp"))
    checks.append(check_freshness("fact_trade", fact_trade, "trade_timestamp"))
    checks.append(
        check_freshness("stg_trading_events", silver_events, "event_timestamp")
    )

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

    with open(report_path, "w") as f:
        json.dump(quality_report, f, indent=2)

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

    run_log_path = RUN_LOG_DIR / f"quality_checks_{run_id}.json"

    with open(run_log_path, "w") as f:
        json.dump(run_log, f, indent=2)

    print("Quality checks completed")
    print(f"status={quality_report['status']}")
    print(f"report={report_path}")
    print(f"run_log={run_log_path}")

    if failed_checks:
        print("Failed checks:")
        for check in failed_checks[:5]:
            print(check)


if __name__ == "__main__":
    run_quality_checks()
