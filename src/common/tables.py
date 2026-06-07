OFFLINE_TABLES = [
    "customers",
    "accounts",
    "securities",
    "orders",
    "trades",
    "cash_transactions",
]

STREAM_TABLES = ["trading_events"]

BRONZE_TABLES = OFFLINE_TABLES + STREAM_TABLES

GOLD_TABLES = [
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
