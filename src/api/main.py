import os

import pandas as pd
from fastapi import FastAPI, Query
from sqlalchemy import create_engine, text

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2://postgres:postgres@localhost:5432/stock_dw",
)

engine = create_engine(DATABASE_URL)

app = FastAPI(title="Stock Trading Serving API")


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/customers/{customer_id}/activity")
def get_customer_activity(customer_id: str):
    query = text("""
        SELECT *
        FROM obt_customer_trading_activity
        WHERE customer_id = :customer_id
        LIMIT 1
    """)

    df = pd.read_sql(query, engine, params={"customer_id": customer_id})

    if df.empty:
        return {"customer_id": customer_id, "data": None}

    return df.iloc[0].to_dict()


@app.get("/securities/top")
def get_top_securities(limit: int = Query(default=10, ge=1, le=100)):
    query = text("""
        SELECT
            security_id,
            ticker,
            COUNT(*) AS total_trades,
            SUM(trade_amount) AS total_trade_amount
        FROM fact_trade
        GROUP BY security_id, ticker
        ORDER BY total_trade_amount DESC
        LIMIT :limit
    """)

    df = pd.read_sql(query, engine, params={"limit": limit})
    return df.to_dict(orient="records")


@app.get("/features/customer/{customer_id}")
def get_customer_features(customer_id: str):
    query = text("""
        SELECT *
        FROM feat_customer_unified
        WHERE customer_id = :customer_id
        LIMIT 1
    """)

    df = pd.read_sql(query, engine, params={"customer_id": customer_id})

    if df.empty:
        return {"customer_id": customer_id, "features": None}

    return df.iloc[0].to_dict()
