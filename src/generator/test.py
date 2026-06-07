import pandas as pd

from src.common.paths import OFFLINE_DIR

orders = pd.read_parquet(OFFLINE_DIR / "orders.parquet")

if __name__ == "__main__":
    security_counts = orders.groupby("ticker").size().sort_values(ascending=False)

    top20_n = int(len(security_counts) * 0.2)

    share = security_counts.head(top20_n).sum() / security_counts.sum()

    print(f"{share:.4f}")
