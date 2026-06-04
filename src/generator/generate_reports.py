import json

import matplotlib.pyplot as plt
import pandas as pd
from config import load_config
from paths import (
    CHARTS_DIR,
    DATA_DIR,
    OFFLINE_DIR,
    REPORTS_DIR,
    SAMPLES_DIR,
    STREAM_EVENTS_PATH,
)

cfg = load_config()

orders = pd.read_parquet(OFFLINE_DIR / "orders.parquet")

REPORTS_DIR.mkdir(parents=True, exist_ok=True)
SAMPLES_DIR.mkdir(parents=True, exist_ok=True)
CHARTS_DIR.mkdir(parents=True, exist_ok=True)


def gen_quality_report():

    report = {
        "orders": len(orders),
        "unique_orders": orders["order_id"].nunique(),
        "duplicate_orders": len(orders) - orders["order_id"].nunique(),
    }

    with open(REPORTS_DIR / "quality_report.json", "w") as f:
        json.dump(report, f, indent=2)


def gen_duplicate_summary():
    unique_rows = orders["order_id"].nunique()
    # duplicate_rows = orders.duplicated(subset=["order_id"], keep=False).sum()
    extra_duplicate_rows = len(orders) - orders["order_id"].nunique()
    duplicated_rows_including_original = orders.duplicated(
        subset=["order_id"],
        keep=False,
    ).sum()
    duplicate_rate_vs_original = extra_duplicate_rows / orders["order_id"].nunique()
    summary = pd.DataFrame(
        [
            {
                "table": "orders",
                "total_rows": len(orders),
                "unique_rows": unique_rows,
                "extra_duplicate_rows": extra_duplicate_rows,
                "duplicated_rows_including_original": duplicated_rows_including_original,
                "duplicate_rate_vs_original": duplicate_rate_vs_original,
            }
        ]
    )

    summary.to_csv(REPORTS_DIR / "duplicate_summary.csv", index=False)


def gen_skew_distribution_csv():
    skew = (
        orders.groupby("ticker")
        .size()
        .reset_index(name="order_count")
        .sort_values("order_count", ascending=False)
    )

    skew.to_csv(REPORTS_DIR / "skew_distribution.csv", index=False)


def gen_data_profile():
    profile = {
        "orders": {
            "rows": len(orders),
            "columns": list(orders.columns),
            "date_min": str(orders["order_timestamp"].min()),
            "date_max": str(orders["order_timestamp"].max()),
            "unique_customers": int(orders["customer_id"].nunique()),
        }
    }

    with open(REPORTS_DIR / "data_profile.json", "w") as f:
        json.dump(profile, f, indent=2)


def gen_skew_summary():
    total_orders = len(orders)

    security_counts = orders.groupby("ticker").size().sort_values(ascending=False)

    top20_n = int(len(security_counts) * 0.2)

    orders_in_top_20_percent = int(security_counts.head(top20_n).sum())

    share = orders_in_top_20_percent / total_orders

    profile = {
        "total_orders": int(total_orders),
        "unique_securities": int(len(security_counts)),
        "top_20_percent_securities": int(top20_n),
        "orders_in_top_20_percent": int(orders_in_top_20_percent),
        "top_20_percent_share": round(float(share), 4),
        "expected_skew_ratio": cfg["top_stock_skew_ratio"],
    }

    with open(REPORTS_DIR / "skew_summary.json", "w") as f:
        json.dump(profile, f, indent=2)


def gen_sample_orders():
    orders.sample(min(1000, len(orders)), random_state=42).to_parquet(
        SAMPLES_DIR / "sample_orders.parquet"
    )


def gen_skew_distributon_png():
    skew = (
        orders.groupby("ticker")
        .size()
        .reset_index(name="order_count")
        .sort_values("order_count", ascending=False)
    )

    top10 = skew.head(10)

    plt.figure(figsize=(10, 5))
    plt.bar(top10["ticker"], top10["order_count"])
    plt.title("Top Securities by Order Count")
    plt.tight_layout()

    plt.savefig(CHARTS_DIR / "skew_distribution.png")


def gen_event_volumne_by_minute_png():
    events = pd.read_json(STREAM_EVENTS_PATH, lines=True)

    events["minute"] = pd.to_datetime(events["event_timestamp"]).dt.floor("min")

    volume = events.groupby("minute").size().reset_index(name="count")

    plt.figure(figsize=(12, 5))
    plt.plot(volume["minute"], volume["count"])

    plt.title("Event Volume by Minute")

    plt.savefig(CHARTS_DIR / "event_volume_by_minute.png")


def gen_sample_trading_events():
    with open(STREAM_PATH) as src:
        sample = [next(src) for _ in range(1000)]

    with open(SAMPLES_DIR / "sample_trading_events.jsonl", "w") as dst:
        dst.writelines(sample)


def gen_stream_summary():
    events = pd.read_json(
        DATA_DIR / "stream" / "trading_events" / "events.jsonl",
        lines=True,
    )

    events["event_timestamp"] = pd.to_datetime(events["event_timestamp"])
    events["created_ts"] = pd.to_datetime(events["created_ts"])

    total_events = len(events)
    unique_events = events["event_id"].nunique()
    extra_duplicate_events = total_events - unique_events

    duplicated_events_including_original = events.duplicated(
        subset=["event_id"],
        keep=False,
    ).sum()

    late_events = events[events["created_ts"] > events["event_timestamp"]]

    delay_seconds = (
        late_events["created_ts"] - late_events["event_timestamp"]
    ).dt.total_seconds()

    event_volume_by_minute = (
        events.assign(event_minute=events["created_ts"].dt.floor("min"))
        .groupby("event_minute")
        .size()
        .reset_index(name="event_count")
        .sort_values("event_minute")
    )

    summary = {
        "total_events": int(total_events),
        "unique_events": int(unique_events),
        "extra_duplicate_events": int(extra_duplicate_events),
        "duplicated_events_including_original": int(
            duplicated_events_including_original
        ),
        "duplicate_event_rate_vs_original": round(
            extra_duplicate_events / unique_events,
            4,
        ),
        "expected_duplicate_rate": cfg["duplicate_rate_stream"],
        "late_events": int(len(late_events)),
        "late_event_rate": round(len(late_events) / total_events, 4),
        "expected_late_arrival_rate": cfg["late_arrival_rate"],
        "avg_late_delay_seconds": round(float(delay_seconds.mean()), 2),
        "max_late_delay_seconds": round(float(delay_seconds.max()), 2),
        "base_events_per_min": cfg["base_events_per_min"],
        "max_events_per_minute": int(event_volume_by_minute["event_count"].max()),
        "avg_events_per_minute": round(
            float(event_volume_by_minute["event_count"].mean()),
            2,
        ),
        "market_open_time": cfg["market_open_time"],
        "market_open_duration_min": cfg["market_open_duration_min"],
        "market_open_burst_multiplier": cfg["market_open_burst_multiplier"],
        "market_close_time": cfg["market_close_time"],
        "market_close_duration_min": cfg["market_close_duration_min"],
        "market_close_burst_multiplier": cfg["market_close_burst_multiplier"],
    }

    with open(REPORTS_DIR / "stream_summary.json", "w") as f:
        json.dump(summary, f, indent=2)

    event_volume_by_minute.to_csv(
        REPORTS_DIR / "event_volume_by_minute.csv",
        index=False,
    )


def gen_schema_evolution_report():
    orders["order_timestamp"] = pd.to_datetime(orders["order_timestamp"])
    schema_change_date = pd.to_datetime(cfg["schema_change_date"])

    before_change = orders[orders["order_timestamp"] < schema_change_date]
    after_change = orders[orders["order_timestamp"] >= schema_change_date]

    report = {
        "schema_change_date": cfg["schema_change_date"],
        "new_column": "order_channel",
        "rows_before_change": int(len(before_change)),
        "rows_after_change": int(len(after_change)),
        "null_order_channel_before_change": int(
            before_change["order_channel"].isna().sum()
        ),
        "null_order_channel_after_change": int(
            after_change["order_channel"].isna().sum()
        ),
        "non_null_order_channel_after_change": int(
            after_change["order_channel"].notna().sum()
        ),
    }

    with open(REPORTS_DIR / "schema_evolution_report.json", "w") as f:
        json.dump(report, f, indent=2)


if __name__ == "__main__":
    print("Generating reports...")
    gen_quality_report()
    gen_duplicate_summary()
    gen_skew_distribution_csv()
    gen_skew_distributon_png()
    gen_skew_summary()
    gen_data_profile()
    gen_sample_orders()
    gen_event_volumne_by_minute_png()
    gen_stream_summary()
    gen_sample_trading_events()
    gen_schema_evolution_report()
    print("DONE!!")
