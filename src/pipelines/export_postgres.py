import sys
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine

sys.path.append(str(Path(__file__).resolve().parents[2]))

from paths import PROJECT_DIR

GOLD_DIR = PROJECT_DIR / "data" / "gold"

DATABASE_URL = "postgresql+psycopg2://postgres:postgres@postgres:5432/stock_dw"

TABLES = [
    "dim_customer",
    "dim_account",
    "dim_security",
    "dim_date",
    "fact_order",
    "fact_trade",
    "fact_cash_transaction",
    "obt_customer_trading_activity",
    "feat_customer_90d",
    "feat_security_1d",
    "feat_stream_customer_60m",
    "feat_customer_unified",
]


def export_table(engine, table_name: str):
    path = GOLD_DIR / f"{table_name}.parquet"

    if not path.exists():
        raise FileNotFoundError(f"Missing Gold table: {path}")

    df = pd.read_parquet(path)

    df.to_sql(
        table_name,
        engine,
        schema="public",
        if_exists="replace",
        index=False,
        chunksize=5000,
    )

    print(f"[{table_name}] exported rows={len(df)}")


def main():
    engine = create_engine(DATABASE_URL)

    for table in TABLES:
        export_table(engine, table)

    print("PostgreSQL export completed")


if __name__ == "__main__":
    main()
