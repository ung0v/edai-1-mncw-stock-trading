import json
import random
import uuid
from datetime import datetime, time, timedelta
from pathlib import Path

import pandas as pd

import sys

sys.path.append(str(Path(__file__).resolve().parents[2]))

from config import load_config
from paths import CONFIG_PATH, OFFLINE_DIR, STREAM_EVENTS_PATH

EVENT_TYPES = [
    "ORDER_PLACED",
    "ORDER_CANCELLED",
    "ORDER_REJECTED",
    "ORDER_MATCHED",
    "PRICE_VIEWED",
    "WATCHLIST_ADDED",
    "LOGIN",
    "LOGOUT",
]

cfg = load_config(str(CONFIG_PATH))

random.seed(cfg["random_seed"])


def parse_hhmm(value: str) -> time:
    hour, minute = map(int, value.split(":"))
    return time(hour=hour, minute=minute)


def is_in_window(current_dt, start_hhmm: str, duration_min: int) -> bool:
    start_t = parse_hhmm(start_hhmm)

    start_dt = current_dt.replace(
        hour=start_t.hour,
        minute=start_t.minute,
        second=0,
        microsecond=0,
    )

    end_dt = start_dt + timedelta(minutes=duration_min)

    return start_dt <= current_dt < end_dt


def get_events_per_min(current_dt, cfg):
    events_per_min = cfg["base_events_per_min"]

    if is_in_window(
        current_dt,
        cfg["market_open_time"],
        cfg["market_open_duration_min"],
    ):
        return events_per_min * cfg["market_open_burst_multiplier"]

    if is_in_window(
        current_dt,
        cfg["market_close_time"],
        cfg["market_close_duration_min"],
    ):
        return events_per_min * cfg["market_close_burst_multiplier"]

    return events_per_min


def generate_one_trading_event(accounts, securities, cfg, current_dt=None):
    current_dt = current_dt or datetime.now()

    account = random.choice(accounts)

    # Skew: 80% events hit top 20% securities
    top_n = max(1, int(len(securities) * 0.2))

    security_pool = (
        securities[:top_n]
        if random.random() < cfg["top_stock_skew_ratio"]
        else securities[top_n:]
    )

    security = random.choice(security_pool or securities)

    event_ts = current_dt

    # Late arrival: event happened earlier, created later
    if random.random() < cfg["late_arrival_rate"]:
        delay_min = random.randint(1, 60)
        event_ts = current_dt - timedelta(minutes=delay_min)

    return {
        "event_id": str(uuid.uuid4()),
        "account_id": account["account_id"],
        "customer_id": account["customer_id"],
        "security_id": security["security_id"],
        "ticker": security["ticker"],
        "event_type": random.choice(EVENT_TYPES),
        "event_timestamp": event_ts.isoformat(),
        "created_ts": current_dt.isoformat(),
        "session_id": str(uuid.uuid4()),
        "device_type": random.choice(["IOS", "ANDROID", "WEB"]),
        "source": random.choice(["APP", "WEB"]),
    }


def generate_stream_batch(accounts, securities, cfg, current_dt=None):
    current_dt = current_dt or datetime.now()

    events_per_min = get_events_per_min(current_dt, cfg)

    events = [
        generate_one_trading_event(accounts, securities, cfg, current_dt)
        for _ in range(events_per_min)
    ]

    # Duplicate stream events
    n_duplicates = int(len(events) * cfg["duplicate_rate_stream"])

    if n_duplicates > 0:
        duplicated_events = random.sample(events, n_duplicates)

        for event in duplicated_events:
            duplicate = event.copy()
            duplicate["created_ts"] = (
                datetime.fromisoformat(event["created_ts"])
                + timedelta(minutes=random.randint(1, 3))
            ).isoformat()

            events.append(duplicate)

    return events


def write_jsonl(events, output_path):
    with open(output_path, "w") as f:
        for event in events:
            f.write(json.dumps(event) + "\n")


def generate_stream_for_duration(accounts, securities, cfg, start_dt, duration_min):
    all_events = []

    for minute_offset in range(duration_min):
        current_dt = start_dt + timedelta(minutes=minute_offset)

        batch = generate_stream_batch(
            accounts=accounts,
            securities=securities,
            cfg=cfg,
            current_dt=current_dt,
        )

        all_events.extend(batch)

    return all_events


if __name__ == "__main__":
    accounts = pd.read_parquet(OFFLINE_DIR / "accounts.parquet").to_dict("records")

    securities = pd.read_parquet(OFFLINE_DIR / "securities.parquet").to_dict("records")

    print("Generating stream data...")

    # Example: generate 1 trading day from 09:00 to 15:00
    start_dt = datetime.now().replace(
        hour=9,
        minute=0,
        second=0,
        microsecond=0,
    )

    events = generate_stream_for_duration(
        accounts=accounts,
        securities=securities,
        cfg=cfg,
        start_dt=start_dt,
        duration_min=360,  # 6 hours
    )

    for event in events[:3]:
        print(json.dumps(event, indent=2))

    STREAM_EVENTS_PATH.parent.mkdir(parents=True, exist_ok=True)
    write_jsonl(events, STREAM_EVENTS_PATH)

    print(f"\nGenerated {len(events)} events")
    print(f"Done! File is in {STREAM_EVENTS_PATH}")
