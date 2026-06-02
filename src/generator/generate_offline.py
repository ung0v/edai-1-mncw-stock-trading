import os
import random
from pathlib import Path

import numpy as np
import pandas as pd
from config import load_config
from faker import Faker

BASE_DIR = Path(__file__).resolve().parent
cfg = load_config(str(BASE_DIR / "config.yaml"))

OUTPUT_DIR = BASE_DIR.parent / "data" / "raw" / "offline"
GENDERS = ["M", "F"]
CITIES = ["Ho Chi Minh City", "Hanoi", "Da Nang", "Can Tho", "Hai Phong"]
OCCUPATIONS = [
    "Software Engineer",
    "Teacher",
    "Doctor",
    "Lawyer",
    "Business Owner",
    "Accountant",
    "Student",
    "Retired",
    "Banker",
    "Government Officer",
]
RISK_PROFILES = ["Conservative", "Moderate", "Aggressive"]
KYC_STATUSES = ["VERIFIED", "PENDING", "REJECTED"]
CUSTOMER_SEGMENTS = ["Retail", "VIP", "Institutional"]
ACCOUNT_TYPES = ["CASH", "MARGIN", "DERIVATIVE"]
ACCOUNT_STATUSES = ["ACTIVE", "SUSPENDED", "CLOSED"]
SECURITY_TYPES = ["STOCK", "ETF", "BOND", "DERIVATIVE"]
EXCHANGES = ["HOSE", "HNX", "UPCOM"]
SECTORS = [
    "Banking",
    "Real Estate",
    "Technology",
    "Retail",
    "Energy",
    "Healthcare",
    "Manufacturing",
    "Finance",
]
ORDER_TYPES = ["LIMIT", "MARKET"]
ORDER_SIDES = ["BUY", "SELL"]
ORDER_STATUSES = ["NEW", "PARTIALLY_FILLED", "FILLED", "CANCELLED", "REJECTED"]
ORDER_QUANTITIES = [100, 200, 500, 1000, 2000, 5000]
FILLABLE_ORDER_STATUSES = ["PARTIALLY_FILLED", "FILLED"]
BOOLEAN_VALUES = [True, False]
CASH_TRANSACTION_TYPES = ["DEPOSIT", "WITHDRAWAL", "FEE", "DIVIDEND"]
CASH_TRANSACTION_STATUSES = ["COMPLETED", "PENDING", "FAILED"]
ON_TIME_EVENT_PROBABILITY = 0.88
MAX_LATE_ARRIVAL_MINUTES = 60
TRADE_EVENT_TYPES = [
    "ORDER_PLACED",
    "ORDER_CANCELLED",
    "ORDER_REJECTED",
    "ORDER_MATCHED",
    "PRICE_VIEWED",
    "WATCHLIST_ADDED",
    "LOGIN",
    "LOGOUT",
]
DEVICE_TYPES = ["IOS", "ANDROID", "WEB"]
EVENT_SOURCES = ["APP", "WEB"]

faker = Faker("vi_VN")
random.seed(cfg["random_seed"])
np.random.seed(cfg["random_seed"])


def random_ts(days_back=cfg["days_history"]):
    return faker.date_time_between(
        start_date=f"-{days_back}d", end_date="now"
    ).isoformat()


OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def write_parquet(records, filename):
    df = pd.DataFrame(records)
    df.to_parquet(OUTPUT_DIR / filename, index=False)
    return df


def gen_customers(n_customers=50000):
    customers = []

    for i in range(n_customers):
        customer = {
            "customer_id": f"CUS{i:06d}",
            "full_name": faker.name(),
            "gender": random.choices(GENDERS, weights=[0.52, 0.48])[0],
            "date_of_birth": faker.date_of_birth(
                minimum_age=18, maximum_age=75
            ).isoformat(),
            "phone_number": faker.phone_number(),
            "email": faker.email(),
            "city": random.choices(CITIES, weights=[0.3, 0.45, 0.10, 0.08, 0.07])[0],
            "province": faker.state(),
            "occupation": random.choice(OCCUPATIONS),
            "risk_profile": random.choices(RISK_PROFILES, weights=[0.4, 0.45, 0.15])[0],
            "kyc_status": random.choices(KYC_STATUSES, weights=[0.95, 0.04, 0.01])[0],
            "customer_segment": random.choices(
                CUSTOMER_SEGMENTS, weights=[0.95, 0.04, 0.01]
            )[0],
            "signup_ts": faker.date_time_between(
                start_date="-2y", end_date="now"
            ).isoformat(),
            "created_ts": faker.date_time_between(
                start_date="-2y", end_date="now"
            ).isoformat(),
            "updated_ts": faker.date_time_between(
                start_date="-30d", end_date="now"
            ).isoformat(),
        }
        customers.append(customer)

    df = write_parquet(customers, "customers.parquet")
    print(f"[customers] {len(df)} rows")
    return customers


def gen_accounts(customers, n_accounts=60000):
    accounts = []

    for i in range(1, n_accounts + 1):
        customer = random.choice(customers)

        accounts.append(
            {
                "account_id": f"ACC{i:06d}",
                "customer_id": customer["customer_id"],
                "account_type": random.choices(
                    ACCOUNT_TYPES, weights=[0.75, 0.20, 0.05]
                )[0],
                "account_status": random.choices(
                    ACCOUNT_STATUSES, weights=[0.96, 0.03, 0.01]
                )[0],
                "opened_ts": random_ts(180),
                "closed_ts": None,
                "created_ts": random_ts(180),
                "updated_ts": random_ts(30),
            }
        )
    df = write_parquet(accounts, "accounts.parquet")
    print(f"[accounts] {len(df)} rows")
    return accounts


def gen_securities(n_securities=500):
    securities = []

    for i in range(1, n_securities + 1):
        ticker = faker.unique.bothify(text="???").upper()

        securities.append(
            {
                "security_id": f"SEC{i:06d}",
                "ticker": ticker,
                "security_name": f"{ticker} Corporation",
                "exchange": random.choices(EXCHANGES, weights=[0.55, 0.30, 0.15])[0],
                "security_type": random.choices(
                    SECURITY_TYPES, weights=[0.85, 0.05, 0.05, 0.05]
                )[0],
                "sector": random.choice(SECTORS),
                "listed_date": faker.date_between(
                    start_date="-15y", end_date="-30d"
                ).isoformat(),
                "is_active": random.choices(BOOLEAN_VALUES, weights=[0.97, 0.03])[0],
                "base_price": round(random.uniform(5000, 150000), 2),
                "created_ts": random_ts(180),
                "updated_ts": random_ts(30),
            }
        )

    df = write_parquet(securities, "securities.parquet")
    print(f"[securities] {len(df)} rows")
    return securities


def gen_orders(accounts, securities, n_orders=200000):
    orders = []

    for i in range(1, n_orders + 1):
        account = random.choice(accounts)
        security = random.choice(securities)

        quantity = random.choice(ORDER_QUANTITIES)
        price = round(security["base_price"] * random.uniform(0.85, 1.15), 2)

        order_ts = random_ts(180)
        status = random.choices(ORDER_STATUSES, weights=[0.10, 0.15, 0.55, 0.15, 0.05])[
            0
        ]

        orders.append(
            {
                "order_id": f"ORD{i:08d}",
                "account_id": account["account_id"],
                "customer_id": account["customer_id"],
                "security_id": security["security_id"],
                "ticker": security["ticker"],
                "order_side": random.choice(ORDER_SIDES),
                "order_type": random.choices(ORDER_TYPES, weights=[0.85, 0.15])[0],
                "order_status": status,
                "order_quantity": quantity,
                "limit_price": price,
                "order_timestamp": order_ts,
                "created_ts": order_ts,
                "updated_ts": random_ts(30),
            }
        )

    df = write_parquet(orders, "orders.parquet")
    print(f"[orders] {len(df)} rows")
    return orders


def gen_trades(orders):
    trades = []
    trade_id = 1

    fillable_orders = [
        o for o in orders if o["order_status"] in FILLABLE_ORDER_STATUSES
    ]

    for order in fillable_orders:
        n_fills = 1 if order["order_status"] == "FILLED" else random.randint(1, 3)

        remaining_qty = order["order_quantity"]

        for _ in range(n_fills):
            if remaining_qty <= 0:
                break

            trade_qty = (
                remaining_qty
                if order["order_status"] == "FILLED"
                else random.randint(100, max(100, remaining_qty))
            )

            trade_price = round(order["limit_price"] * random.uniform(0.995, 1.005), 2)

            trades.append(
                {
                    "trade_id": f"TRD{trade_id:08d}",
                    "order_id": order["order_id"],
                    "account_id": order["account_id"],
                    "customer_id": order["customer_id"],
                    "security_id": order["security_id"],
                    "ticker": order["ticker"],
                    "trade_side": order["order_side"],
                    "trade_quantity": trade_qty,
                    "trade_price": trade_price,
                    "trade_amount": round(trade_qty * trade_price, 2),
                    "fee_amount": round(trade_qty * trade_price * 0.0015, 2),
                    "trade_timestamp": random_ts(180),
                    "created_ts": random_ts(180),
                }
            )

            trade_id += 1
            remaining_qty -= trade_qty

    df = write_parquet(trades, "trades.parquet")
    print(f"[trades] {len(df)} rows")
    return trades


def gen_cash_transactions(accounts, trades, n_extra_cash=50000):
    cash_transactions = []
    tx_id = 1

    # Cash movements from trades
    for trade in trades:
        amount = trade["trade_amount"] + trade["fee_amount"]

        cash_transactions.append(
            {
                "cash_transaction_id": f"CAS{tx_id:08d}",
                "account_id": trade["account_id"],
                "customer_id": trade["customer_id"],
                "trade_id": trade["trade_id"],
                "transaction_type": "TRADE_BUY"
                if trade["trade_side"] == "BUY"
                else "TRADE_SELL",
                "amount": -amount if trade["trade_side"] == "BUY" else amount,
                "currency": "VND",
                "transaction_status": "COMPLETED",
                "transaction_timestamp": trade["trade_timestamp"],
                "created_ts": trade["created_ts"],
            }
        )
        tx_id += 1

    # Deposits / withdrawals
    for _ in range(n_extra_cash):
        account = random.choice(accounts)
        tx_type = random.choices(
            CASH_TRANSACTION_TYPES, weights=[0.50, 0.30, 0.15, 0.05]
        )[0]

        amount = round(random.uniform(100_000, 200_000_000), 2)

        cash_transactions.append(
            {
                "cash_transaction_id": f"CAS{tx_id:08d}",
                "account_id": account["account_id"],
                "customer_id": account["customer_id"],
                "trade_id": None,
                "transaction_type": tx_type,
                "amount": -amount if tx_type in ("WITHDRAWAL", "FEE") else amount,
                "currency": "VND",
                "transaction_status": random.choices(
                    CASH_TRANSACTION_STATUSES, weights=[0.94, 0.04, 0.02]
                )[0],
                "transaction_timestamp": random_ts(180),
                "created_ts": random_ts(180),
            }
        )

        tx_id += 1

    df = write_parquet(cash_transactions, "cash_transactions.parquet")
    print(f"[cash_transactions] {len(df)} rows")
    return cash_transactions


if __name__ == "__main__":
    print("Generating offline data...")
    customers = gen_customers(n_customers=cfg["n_customers"])
    accounts = gen_accounts(customers, n_accounts=cfg["n_accounts"])
    securities = gen_securities(n_securities=cfg["n_securities"])
    orders = gen_orders(accounts, securities, n_orders=cfg["n_orders"])
    trades = gen_trades(orders)
    cash_transactions = gen_cash_transactions(accounts, trades)
    print("\nDone! Files are in ./data/offline")
