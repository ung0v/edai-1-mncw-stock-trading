import sys
from pathlib import Path

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[2]))


from paths import PROJECT_DIR

GOLD_DIR = PROJECT_DIR / "data" / "gold"
SILVER_DIR = PROJECT_DIR / "data" / "silver"


def read_gold(table_name: str) -> pd.DataFrame:
    return pd.read_parquet(GOLD_DIR / f"{table_name}.parquet")


def read_silver(table_name: str) -> pd.DataFrame:
    return pd.read_parquet(SILVER_DIR / f"stg_{table_name}.parquet")


def write_gold(table_name: str, df: pd.DataFrame):
    path = GOLD_DIR / f"{table_name}.parquet"
    df.to_parquet(path, index=False)
    return path


def current_utc_ts():
    return pd.Timestamp.now(tz="UTC")


def build_feat_customer_90d(fact_order: pd.DataFrame, fact_trade: pd.DataFrame):
    orders = fact_order.copy()
    trades = fact_trade.copy()

    orders["order_timestamp"] = pd.to_datetime(orders["order_timestamp"])
    trades["trade_timestamp"] = pd.to_datetime(trades["trade_timestamp"])

    reference_ts = max(
        orders["order_timestamp"].max(),
        trades["trade_timestamp"].max(),
    )

    window_start = reference_ts - pd.Timedelta(days=90)

    orders_90d = orders[orders["order_timestamp"].between(window_start, reference_ts)]
    trades_90d = trades[trades["trade_timestamp"].between(window_start, reference_ts)]

    order_features = (
        orders_90d.groupby("customer_id")
        .agg(
            f_customer_order_count_90d=("order_id", "count"),
            f_customer_buy_order_count_90d=("order_side", lambda x: (x == "BUY").sum()),
            f_customer_sell_order_count_90d=(
                "order_side",
                lambda x: (x == "SELL").sum(),
            ),
            f_customer_total_order_amount_90d=("order_amount", "sum"),
            f_customer_avg_order_amount_90d=("order_amount", "mean"),
            f_customer_distinct_ticker_90d=("ticker", "nunique"),
        )
        .reset_index()
    )

    trade_features = (
        trades_90d.groupby("customer_id")
        .agg(
            f_customer_trade_count_90d=("trade_id", "count"),
            f_customer_total_trade_amount_90d=("trade_amount", "sum"),
            f_customer_total_fee_amount_90d=("fee_amount", "sum"),
        )
        .reset_index()
    )

    feat = order_features.merge(trade_features, on="customer_id", how="outer")
    feat = feat.fillna(0)

    feat["event_timestamp"] = reference_ts
    feat["created_ts"] = current_utc_ts()

    return feat


def build_feat_security_1d(fact_order: pd.DataFrame, fact_trade: pd.DataFrame):
    orders = fact_order.copy()
    trades = fact_trade.copy()

    orders["order_timestamp"] = pd.to_datetime(orders["order_timestamp"])
    trades["trade_timestamp"] = pd.to_datetime(trades["trade_timestamp"])

    reference_ts = max(
        orders["order_timestamp"].max(),
        trades["trade_timestamp"].max(),
    )

    window_start = reference_ts - pd.Timedelta(days=1)

    orders_1d = orders[orders["order_timestamp"].between(window_start, reference_ts)]
    trades_1d = trades[trades["trade_timestamp"].between(window_start, reference_ts)]

    order_features = (
        orders_1d.groupby("security_id")
        .agg(
            f_security_order_count_1d=("order_id", "count"),
            f_security_buy_order_count_1d=("order_side", lambda x: (x == "BUY").sum()),
            f_security_sell_order_count_1d=(
                "order_side",
                lambda x: (x == "SELL").sum(),
            ),
            f_security_total_order_amount_1d=("order_amount", "sum"),
            f_security_distinct_customer_1d=("customer_id", "nunique"),
        )
        .reset_index()
    )

    trade_features = (
        trades_1d.groupby("security_id")
        .agg(
            f_security_trade_count_1d=("trade_id", "count"),
            f_security_total_trade_amount_1d=("trade_amount", "sum"),
            f_security_avg_trade_price_1d=("trade_price", "mean"),
        )
        .reset_index()
    )

    feat = order_features.merge(trade_features, on="security_id", how="outer")
    feat = feat.fillna(0)

    feat["event_timestamp"] = reference_ts
    feat["created_ts"] = current_utc_ts()

    return feat


def build_feat_stream_customer_60m(events: pd.DataFrame):
    df = events.copy()

    df["event_timestamp"] = pd.to_datetime(df["event_timestamp"])
    df["created_ts"] = pd.to_datetime(df["created_ts"])

    reference_ts = df["event_timestamp"].max()
    window_start = reference_ts - pd.Timedelta(minutes=60)

    events_60m = df[df["event_timestamp"].between(window_start, reference_ts)]

    feat = (
        events_60m.groupby("customer_id")
        .agg(
            f_stream_event_count_60m=("event_id", "count"),
            f_stream_order_placed_count_60m=(
                "event_type",
                lambda x: (x == "ORDER_PLACED").sum(),
            ),
            f_stream_order_matched_count_60m=(
                "event_type",
                lambda x: (x == "ORDER_MATCHED").sum(),
            ),
            f_stream_price_viewed_count_60m=(
                "event_type",
                lambda x: (x == "PRICE_VIEWED").sum(),
            ),
            f_stream_distinct_ticker_60m=("ticker", "nunique"),
            f_stream_late_event_count_60m=("is_late_arrival", "sum"),
        )
        .reset_index()
    )

    feat["event_timestamp"] = reference_ts
    feat["created_ts"] = current_utc_ts()

    return feat


def build_feat_customer_unified(
    feat_customer_90d: pd.DataFrame,
    feat_stream_customer_60m: pd.DataFrame,
):
    unified = feat_customer_90d.merge(
        feat_stream_customer_60m,
        on="customer_id",
        how="outer",
        suffixes=("_offline", "_stream"),
    )

    unified["event_timestamp_offline"] = pd.to_datetime(
        unified["event_timestamp_offline"], utc=True, errors="coerce"
    )
    unified["event_timestamp_stream"] = pd.to_datetime(
        unified["event_timestamp_stream"], utc=True, errors="coerce"
    )

    numeric_cols = unified.select_dtypes(include="number").columns
    unified[numeric_cols] = unified[numeric_cols].fillna(0)

    unified["event_timestamp"] = unified["event_timestamp_offline"].combine_first(
        unified["event_timestamp_stream"]
    )

    unified["created_ts"] = current_utc_ts()

    return unified


def run_feature_transform():
    fact_order = read_gold("fact_order")
    fact_trade = read_gold("fact_trade")
    events = read_silver("trading_events")

    feat_customer_90d = build_feat_customer_90d(fact_order, fact_trade)
    feat_security_1d = build_feat_security_1d(fact_order, fact_trade)
    feat_stream_customer_60m = build_feat_stream_customer_60m(events)

    feat_customer_unified = build_feat_customer_unified(
        feat_customer_90d=feat_customer_90d,
        feat_stream_customer_60m=feat_stream_customer_60m,
    )

    outputs = {
        "feat_customer_90d": feat_customer_90d,
        "feat_security_1d": feat_security_1d,
        "feat_stream_customer_60m": feat_stream_customer_60m,
        "feat_customer_unified": feat_customer_unified,
    }

    print("Feature transform completed")

    for table_name, df in outputs.items():
        path = write_gold(table_name, df)
        print(f"[{table_name}] rows={len(df)} output={path}")


if __name__ == "__main__":
    run_feature_transform()
