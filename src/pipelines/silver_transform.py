import sys
from pathlib import Path

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[2]))

from paths import PROJECT_DIR

BRONZE_DIR = PROJECT_DIR / "data" / "bronze"
SILVER_DIR = PROJECT_DIR / "data" / "silver"


TABLES = [
    "customers",
    "accounts",
    "securities",
    "orders",
    "trades",
    "cash_transactions",
    "trading_events",
]


def read_latest_bronze_table(table_name: str) -> pd.DataFrame:
    table_dir = BRONZE_DIR / table_name

    if not table_dir.exists():
        raise FileNotFoundError(f"Missing bronze table directory: {table_dir}")

    files = sorted(table_dir.glob("*.parquet"))

    if not files:
        raise FileNotFoundError(f"No bronze parquet files found for {table_name}")

    latest_file = files[-1]
    return pd.read_parquet(latest_file)


def standardize_timestamps(
    df: pd.DataFrame, timestamp_columns: list[str]
) -> pd.DataFrame:
    df = df.copy()

    for col in timestamp_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    return df


def dedup_latest(
    df: pd.DataFrame, key: str, order_col: str = "created_ts"
) -> pd.DataFrame:
    if key not in df.columns:
        return df

    if order_col not in df.columns:
        return df.drop_duplicates(subset=[key], keep="last")

    return (
        df.sort_values(order_col)
        .drop_duplicates(subset=[key], keep="last")
        .reset_index(drop=True)
    )


def clean_customers(df: pd.DataFrame) -> pd.DataFrame:
    df = standardize_timestamps(df, ["signup_ts", "created_ts", "updated_ts"])
    df = dedup_latest(df, "customer_id", "updated_ts")
    return df


def clean_accounts(df: pd.DataFrame) -> pd.DataFrame:
    df = standardize_timestamps(
        df, ["opened_ts", "closed_ts", "created_ts", "updated_ts"]
    )
    df = dedup_latest(df, "account_id", "updated_ts")
    return df


def clean_securities(df: pd.DataFrame) -> pd.DataFrame:
    df = standardize_timestamps(df, ["listed_date", "created_ts", "updated_ts"])
    df = dedup_latest(df, "security_id", "updated_ts")
    return df


def clean_orders(df: pd.DataFrame) -> pd.DataFrame:
    df = standardize_timestamps(df, ["order_timestamp", "created_ts", "updated_ts"])

    df["order_quantity"] = pd.to_numeric(df["order_quantity"], errors="coerce")
    df["limit_price"] = pd.to_numeric(df["limit_price"], errors="coerce")

    df = df[df["order_id"].notna()]
    df = df[df["account_id"].notna()]
    df = df[df["customer_id"].notna()]
    df = df[df["security_id"].notna()]
    df = df[df["order_timestamp"].notna()]

    df = dedup_latest(df, "order_id", "created_ts")

    return df


def clean_trades(df: pd.DataFrame) -> pd.DataFrame:
    df = standardize_timestamps(df, ["trade_timestamp", "created_ts"])

    df["trade_quantity"] = pd.to_numeric(df["trade_quantity"], errors="coerce")
    df["trade_price"] = pd.to_numeric(df["trade_price"], errors="coerce")
    df["trade_amount"] = pd.to_numeric(df["trade_amount"], errors="coerce")
    df["fee_amount"] = pd.to_numeric(df["fee_amount"], errors="coerce")

    df = df[df["trade_id"].notna()]
    df = df[df["order_id"].notna()]
    df = df[df["trade_timestamp"].notna()]

    df = dedup_latest(df, "trade_id", "created_ts")

    return df


def clean_cash_transactions(df: pd.DataFrame) -> pd.DataFrame:
    df = standardize_timestamps(df, ["transaction_timestamp", "created_ts"])

    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")

    df = df[df["cash_transaction_id"].notna()]
    df = df[df["account_id"].notna()]
    df = df[df["customer_id"].notna()]
    df = df[df["transaction_timestamp"].notna()]

    df = dedup_latest(df, "cash_transaction_id", "created_ts")

    return df


def clean_trading_events(df: pd.DataFrame) -> pd.DataFrame:
    df = standardize_timestamps(df, ["event_timestamp", "created_ts"])

    df = df[df["event_id"].notna()]
    df = df[df["account_id"].notna()]
    df = df[df["customer_id"].notna()]
    df = df[df["event_timestamp"].notna()]
    df = df[df["created_ts"].notna()]

    df = dedup_latest(df, "event_id", "created_ts")

    df["is_late_arrival"] = df["created_ts"] > df["event_timestamp"]
    df["arrival_delay_seconds"] = (
        df["created_ts"] - df["event_timestamp"]
    ).dt.total_seconds()

    return df


CLEANERS = {
    "customers": clean_customers,
    "accounts": clean_accounts,
    "securities": clean_securities,
    "orders": clean_orders,
    "trades": clean_trades,
    "cash_transactions": clean_cash_transactions,
    "trading_events": clean_trading_events,
}


def write_silver_table(table_name: str, df: pd.DataFrame):
    SILVER_DIR.mkdir(parents=True, exist_ok=True)

    output_path = SILVER_DIR / f"stg_{table_name}.parquet"
    df.to_parquet(output_path, index=False)

    return output_path


def run_silver_transform():
    results = []

    for table in TABLES:
        bronze_df = read_latest_bronze_table(table)
        cleaner = CLEANERS[table]

        silver_df = cleaner(bronze_df)
        output_path = write_silver_table(table, silver_df)

        results.append(
            {
                "table": table,
                "bronze_rows": len(bronze_df),
                "silver_rows": len(silver_df),
                "removed_rows": len(bronze_df) - len(silver_df),
                "output_path": str(output_path),
            }
        )

    print("Silver transform completed")

    for result in results:
        print(
            f"[{result['table']}] "
            f"bronze={result['bronze_rows']} "
            f"silver={result['silver_rows']} "
            f"removed={result['removed_rows']} "
            f"output={result['output_path']}"
        )


if __name__ == "__main__":
    run_silver_transform()
