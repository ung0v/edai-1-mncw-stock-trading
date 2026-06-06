import sys
from pathlib import Path

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[2]))

from paths import PROJECT_DIR

SILVER_DIR = PROJECT_DIR / "data" / "silver"
GOLD_DIR = PROJECT_DIR / "data" / "gold"


def read_silver(table_name: str) -> pd.DataFrame:
    path = SILVER_DIR / f"stg_{table_name}.parquet"

    if not path.exists():
        raise FileNotFoundError(f"Missing silver table: {path}")

    return pd.read_parquet(path)


def write_gold(table_name: str, df: pd.DataFrame):
    GOLD_DIR.mkdir(parents=True, exist_ok=True)

    path = GOLD_DIR / f"{table_name}.parquet"
    df.to_parquet(path, index=False)

    return path


def build_dim_customer(customers: pd.DataFrame) -> pd.DataFrame:
    df = customers.copy()
    df = df.sort_values("customer_id").reset_index(drop=True)

    df.insert(0, "customer_key", range(1, len(df) + 1))

    return df[
        [
            "customer_key",
            "customer_id",
            "full_name",
            "gender",
            "date_of_birth",
            "city",
            "province",
            "occupation",
            "risk_profile",
            "kyc_status",
            "customer_segment",
            "signup_ts",
            "created_ts",
            "updated_ts",
        ]
    ]


def build_dim_account(accounts: pd.DataFrame) -> pd.DataFrame:
    df = accounts.copy()
    df = df.sort_values("account_id").reset_index(drop=True)

    df.insert(0, "account_key", range(1, len(df) + 1))

    return df[
        [
            "account_key",
            "account_id",
            "customer_id",
            "account_type",
            "account_status",
            "opened_ts",
            "closed_ts",
            "created_ts",
            "updated_ts",
        ]
    ]


def build_dim_security(securities: pd.DataFrame) -> pd.DataFrame:
    df = securities.copy()
    df = df.sort_values("security_id").reset_index(drop=True)

    df.insert(0, "security_key", range(1, len(df) + 1))

    return df[
        [
            "security_key",
            "security_id",
            "ticker",
            "security_name",
            "exchange",
            "security_type",
            "sector",
            "listed_date",
            "is_active",
            "base_price",
            "created_ts",
            "updated_ts",
        ]
    ]


def build_dim_date(*timestamp_series: pd.Series) -> pd.DataFrame:
    all_dates = []

    for series in timestamp_series:
        dates = pd.to_datetime(series, errors="coerce").dt.date.dropna()
        all_dates.extend(dates.tolist())

    unique_dates = sorted(set(all_dates))

    df = pd.DataFrame({"calendar_date": pd.to_datetime(unique_dates)})

    df["date_key"] = df["calendar_date"].dt.strftime("%Y%m%d").astype(int)
    df["day_of_week"] = df["calendar_date"].dt.day_name()
    df["month"] = df["calendar_date"].dt.month
    df["quarter"] = df["calendar_date"].dt.quarter
    df["year"] = df["calendar_date"].dt.year
    df["is_weekend"] = df["calendar_date"].dt.dayofweek >= 5

    return df[
        [
            "date_key",
            "calendar_date",
            "day_of_week",
            "month",
            "quarter",
            "year",
            "is_weekend",
        ]
    ]


def build_fact_order(
    orders: pd.DataFrame,
    dim_customer: pd.DataFrame,
    dim_account: pd.DataFrame,
    dim_security: pd.DataFrame,
) -> pd.DataFrame:
    df = orders.copy()

    df["order_date_key"] = (
        pd.to_datetime(df["order_timestamp"]).dt.strftime("%Y%m%d").astype(int)
    )
    df["order_amount"] = df["order_quantity"] * df["limit_price"]

    df = df.merge(
        dim_customer[["customer_key", "customer_id"]],
        on="customer_id",
        how="left",
    )

    df = df.merge(
        dim_account[["account_key", "account_id"]],
        on="account_id",
        how="left",
    )

    df = df.merge(
        dim_security[["security_key", "security_id"]],
        on="security_id",
        how="left",
    )

    return df[
        [
            "order_id",
            "customer_key",
            "account_key",
            "security_key",
            "order_date_key",
            "customer_id",
            "account_id",
            "security_id",
            "ticker",
            "order_side",
            "order_type",
            "order_status",
            "order_quantity",
            "limit_price",
            "order_amount",
            "order_channel",
            "order_timestamp",
            "created_ts",
            "updated_ts",
        ]
    ]


def build_fact_trade(
    trades: pd.DataFrame,
    dim_customer: pd.DataFrame,
    dim_account: pd.DataFrame,
    dim_security: pd.DataFrame,
) -> pd.DataFrame:
    df = trades.copy()

    df["trade_date_key"] = (
        pd.to_datetime(df["trade_timestamp"]).dt.strftime("%Y%m%d").astype(int)
    )

    df = df.merge(
        dim_customer[["customer_key", "customer_id"]],
        on="customer_id",
        how="left",
    )

    df = df.merge(
        dim_account[["account_key", "account_id"]],
        on="account_id",
        how="left",
    )

    df = df.merge(
        dim_security[["security_key", "security_id"]],
        on="security_id",
        how="left",
    )

    return df[
        [
            "trade_id",
            "order_id",
            "customer_key",
            "account_key",
            "security_key",
            "trade_date_key",
            "customer_id",
            "account_id",
            "security_id",
            "ticker",
            "trade_side",
            "trade_quantity",
            "trade_price",
            "trade_amount",
            "fee_amount",
            "trade_timestamp",
            "created_ts",
        ]
    ]


def build_fact_cash_transaction(
    cash_transactions: pd.DataFrame,
    dim_customer: pd.DataFrame,
    dim_account: pd.DataFrame,
) -> pd.DataFrame:
    df = cash_transactions.copy()

    df["transaction_date_key"] = (
        pd.to_datetime(df["transaction_timestamp"]).dt.strftime("%Y%m%d").astype(int)
    )

    df = df.merge(
        dim_customer[["customer_key", "customer_id"]],
        on="customer_id",
        how="left",
    )

    df = df.merge(
        dim_account[["account_key", "account_id"]],
        on="account_id",
        how="left",
    )

    return df[
        [
            "cash_transaction_id",
            "customer_key",
            "account_key",
            "transaction_date_key",
            "account_id",
            "customer_id",
            "trade_id",
            "transaction_type",
            "amount",
            "currency",
            "transaction_status",
            "transaction_timestamp",
            "created_ts",
        ]
    ]


def build_obt_customer_trading_activity(
    dim_customer: pd.DataFrame,
    fact_order: pd.DataFrame,
    fact_trade: pd.DataFrame,
    fact_cash_transaction: pd.DataFrame,
) -> pd.DataFrame:
    order_agg = (
        fact_order.groupby("customer_id")
        .agg(
            total_orders=("order_id", "count"),
            total_buy_orders=("order_side", lambda x: (x == "BUY").sum()),
            total_sell_orders=("order_side", lambda x: (x == "SELL").sum()),
            total_order_amount=("order_amount", "sum"),
            first_order_ts=("order_timestamp", "min"),
            last_order_ts=("order_timestamp", "max"),
        )
        .reset_index()
    )

    trade_agg = (
        fact_trade.groupby("customer_id")
        .agg(
            total_trades=("trade_id", "count"),
            total_trade_amount=("trade_amount", "sum"),
            total_fee_amount=("fee_amount", "sum"),
            first_trade_ts=("trade_timestamp", "min"),
            last_trade_ts=("trade_timestamp", "max"),
        )
        .reset_index()
    )

    cash_agg = (
        fact_cash_transaction.groupby("customer_id")
        .agg(
            total_cash_transactions=("cash_transaction_id", "count"),
            net_cash_amount=("amount", "sum"),
        )
        .reset_index()
    )

    obt = dim_customer[
        [
            "customer_key",
            "customer_id",
            "city",
            "risk_profile",
            "kyc_status",
            "customer_segment",
            "signup_ts",
        ]
    ].copy()

    obt = obt.merge(order_agg, on="customer_id", how="left")
    obt = obt.merge(trade_agg, on="customer_id", how="left")
    obt = obt.merge(cash_agg, on="customer_id", how="left")

    metric_cols = [
        "total_orders",
        "total_buy_orders",
        "total_sell_orders",
        "total_order_amount",
        "total_trades",
        "total_trade_amount",
        "total_fee_amount",
        "total_cash_transactions",
        "net_cash_amount",
    ]

    for col in metric_cols:
        obt[col] = obt[col].fillna(0)

    return obt


def run_gold_transform():
    customers = read_silver("customers")
    accounts = read_silver("accounts")
    securities = read_silver("securities")
    orders = read_silver("orders")
    trades = read_silver("trades")
    cash_transactions = read_silver("cash_transactions")

    dim_customer = build_dim_customer(customers)
    dim_account = build_dim_account(accounts)
    dim_security = build_dim_security(securities)

    dim_date = build_dim_date(
        orders["order_timestamp"],
        trades["trade_timestamp"],
        cash_transactions["transaction_timestamp"],
    )

    fact_order = build_fact_order(
        orders=orders,
        dim_customer=dim_customer,
        dim_account=dim_account,
        dim_security=dim_security,
    )

    fact_trade = build_fact_trade(
        trades=trades,
        dim_customer=dim_customer,
        dim_account=dim_account,
        dim_security=dim_security,
    )

    fact_cash_transaction = build_fact_cash_transaction(
        cash_transactions=cash_transactions,
        dim_customer=dim_customer,
        dim_account=dim_account,
    )

    obt_customer_trading_activity = build_obt_customer_trading_activity(
        dim_customer=dim_customer,
        fact_order=fact_order,
        fact_trade=fact_trade,
        fact_cash_transaction=fact_cash_transaction,
    )

    outputs = {
        "dim_customer": dim_customer,
        "dim_account": dim_account,
        "dim_security": dim_security,
        "dim_date": dim_date,
        "fact_order": fact_order,
        "fact_trade": fact_trade,
        "fact_cash_transaction": fact_cash_transaction,
        "obt_customer_trading_activity": obt_customer_trading_activity,
    }

    print("Gold transform completed")

    for table_name, df in outputs.items():
        path = write_gold(table_name, df)
        print(f"[{table_name}] rows={len(df)} output={path}")


if __name__ == "__main__":
    run_gold_transform()
