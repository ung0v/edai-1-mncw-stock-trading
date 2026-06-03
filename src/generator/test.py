from pathlib import Path

import pandas as pd

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR.parent / "data" / "raw"
OFFLINE_DIR = DATA_DIR / "offline"
STREAM_PATH = DATA_DIR / "stream" / "trading_events" / "events.jsonl"
REPORTS_DIR = BASE_DIR / "outputs" / "reports"
SAMPLES_DIR = BASE_DIR / "outputs" / "samples"
CHARTS_DIR = BASE_DIR / "outputs" / "charts"

orders = pd.read_parquet(OFFLINE_DIR / "orders.parquet")

if __name__ == "__main__":
    security_counts = orders.groupby("ticker").size().sort_values(ascending=False)

    top20_n = int(len(security_counts) * 0.2)

    share = security_counts.head(top20_n).sum() / security_counts.sum()

    print(f"{share:.4f}")
