from pyspark.sql import Window
from pyspark.sql import functions as F

from src.common.spark import get_spark_session, layer_path, write_delta


def read_silver(spark, table_name: str):
    return spark.read.format("delta").load(layer_path("silver", f"stg_{table_name}"))


def write_gold(table_name: str, df, partition_by: list[str] | None = None):
    output_path = layer_path("gold", table_name)
    write_delta(df, output_path, mode="overwrite", partition_by=partition_by)
    return output_path


def add_surrogate_key(df, business_key: str, surrogate_key: str):
    window = Window.orderBy(F.col(business_key))
    return df.orderBy(business_key).withColumn(surrogate_key, F.row_number().over(window))


def build_dim_customer(customers):
    return add_surrogate_key(customers, "customer_id", "customer_key").select(
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
    )


def build_dim_account(accounts):
    return add_surrogate_key(accounts, "account_id", "account_key").select(
        "account_key",
        "account_id",
        "customer_id",
        "account_type",
        "account_status",
        "opened_ts",
        "closed_ts",
        "created_ts",
        "updated_ts",
    )


def build_dim_security(securities):
    return add_surrogate_key(securities, "security_id", "security_key").select(
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
    )


def build_dim_date(orders, trades, cash_transactions):
    dates = (
        orders.select(F.to_date("order_timestamp").alias("calendar_date"))
        .unionByName(trades.select(F.to_date("trade_timestamp").alias("calendar_date")))
        .unionByName(cash_transactions.select(F.to_date("transaction_timestamp").alias("calendar_date")))
        .filter(F.col("calendar_date").isNotNull())
        .distinct()
    )

    return dates.select(
        F.date_format("calendar_date", "yyyyMMdd").cast("int").alias("date_key"),
        "calendar_date",
        F.date_format("calendar_date", "EEEE").alias("day_of_week"),
        F.month("calendar_date").alias("month"),
        F.quarter("calendar_date").alias("quarter"),
        F.year("calendar_date").alias("year"),
        F.dayofweek("calendar_date").isin([1, 7]).alias("is_weekend"),
    )


def build_fact_order(orders, dim_customer, dim_account, dim_security):
    customer_keys = dim_customer.select("customer_id", "customer_key")
    account_keys = dim_account.select("account_id", "account_key")
    security_keys = dim_security.select(
        "security_id",
        "security_key",
        F.col("ticker").alias("dim_ticker"),
    )

    return (
        orders.withColumn("order_date_key", F.date_format("order_timestamp", "yyyyMMdd").cast("int"))
        .withColumn("order_amount", F.col("order_quantity") * F.col("limit_price"))
        .join(customer_keys, on="customer_id", how="left")
        .join(account_keys, on="account_id", how="left")
        .join(security_keys, on="security_id", how="left")
        .select(
            "order_id",
            "customer_key",
            "account_key",
            "security_key",
            "order_date_key",
            "customer_id",
            "account_id",
            "security_id",
            F.coalesce(F.col("ticker"), F.col("dim_ticker")).alias("ticker"),
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
        )
    )


def build_fact_trade(trades, dim_customer, dim_account, dim_security):
    customer_keys = dim_customer.select("customer_id", "customer_key")
    account_keys = dim_account.select("account_id", "account_key")
    security_keys = dim_security.select(
        "security_id",
        "security_key",
        F.col("ticker").alias("dim_ticker"),
    )

    return (
        trades.withColumn("trade_date_key", F.date_format("trade_timestamp", "yyyyMMdd").cast("int"))
        .join(customer_keys, on="customer_id", how="left")
        .join(account_keys, on="account_id", how="left")
        .join(security_keys, on="security_id", how="left")
        .select(
            "trade_id",
            "order_id",
            "customer_key",
            "account_key",
            "security_key",
            "trade_date_key",
            "customer_id",
            "account_id",
            "security_id",
            F.coalesce(F.col("ticker"), F.col("dim_ticker")).alias("ticker"),
            "trade_side",
            "trade_quantity",
            "trade_price",
            "trade_amount",
            "fee_amount",
            "trade_timestamp",
            "created_ts",
        )
    )


def build_fact_cash_transaction(cash_transactions, dim_customer, dim_account):
    customer_keys = dim_customer.select("customer_id", "customer_key")
    account_keys = dim_account.select("account_id", "account_key")

    return (
        cash_transactions.withColumn("transaction_date_key", F.date_format("transaction_timestamp", "yyyyMMdd").cast("int"))
        .join(customer_keys, on="customer_id", how="left")
        .join(account_keys, on="account_id", how="left")
        .select(
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
        )
    )


def build_obt_customer_trading_activity(dim_customer, fact_order, fact_trade, fact_cash_transaction):
    order_agg = fact_order.groupBy("customer_id").agg(
        F.count("order_id").alias("total_orders"),
        F.sum(F.when(F.col("order_side") == "BUY", 1).otherwise(0)).alias("total_buy_orders"),
        F.sum(F.when(F.col("order_side") == "SELL", 1).otherwise(0)).alias("total_sell_orders"),
        F.sum("order_amount").alias("total_order_amount"),
        F.min("order_timestamp").alias("first_order_ts"),
        F.max("order_timestamp").alias("last_order_ts"),
    )

    trade_agg = fact_trade.groupBy("customer_id").agg(
        F.count("trade_id").alias("total_trades"),
        F.sum("trade_amount").alias("total_trade_amount"),
        F.sum("fee_amount").alias("total_fee_amount"),
        F.min("trade_timestamp").alias("first_trade_ts"),
        F.max("trade_timestamp").alias("last_trade_ts"),
    )

    cash_agg = fact_cash_transaction.groupBy("customer_id").agg(
        F.count("cash_transaction_id").alias("total_cash_transactions"),
        F.sum("amount").alias("net_cash_amount"),
    )

    return (
        dim_customer.select(
            "customer_key",
            "customer_id",
            "city",
            "risk_profile",
            "kyc_status",
            "customer_segment",
            "signup_ts",
        )
        .join(order_agg, on="customer_id", how="left")
        .join(trade_agg, on="customer_id", how="left")
        .join(cash_agg, on="customer_id", how="left")
        .fillna(
            0,
            subset=[
                "total_orders",
                "total_buy_orders",
                "total_sell_orders",
                "total_order_amount",
                "total_trades",
                "total_trade_amount",
                "total_fee_amount",
                "total_cash_transactions",
                "net_cash_amount",
            ],
        )
    )


def run_gold_transform():
    spark = get_spark_session("gold-transform")

    customers = read_silver(spark, "customers")
    accounts = read_silver(spark, "accounts")
    securities = read_silver(spark, "securities")
    orders = read_silver(spark, "orders")
    trades = read_silver(spark, "trades")
    cash_transactions = read_silver(spark, "cash_transactions")

    dim_customer = build_dim_customer(customers)
    dim_account = build_dim_account(accounts)
    dim_security = build_dim_security(securities)
    dim_date = build_dim_date(orders, trades, cash_transactions)

    fact_order = build_fact_order(orders, dim_customer, dim_account, dim_security)
    fact_trade = build_fact_trade(trades, dim_customer, dim_account, dim_security)
    fact_cash_transaction = build_fact_cash_transaction(cash_transactions, dim_customer, dim_account)
    obt_customer_trading_activity = build_obt_customer_trading_activity(
        dim_customer, fact_order, fact_trade, fact_cash_transaction
    )

    outputs = {
        "dim_customer": (dim_customer, None),
        "dim_account": (dim_account, None),
        "dim_security": (dim_security, None),
        "dim_date": (dim_date, None),
        "fact_order": (fact_order.withColumn("order_date", F.to_date("order_timestamp")), ["order_date"]),
        "fact_trade": (fact_trade.withColumn("trade_date", F.to_date("trade_timestamp")), ["trade_date"]),
        "fact_cash_transaction": (
            fact_cash_transaction.withColumn("transaction_date", F.to_date("transaction_timestamp")),
            ["transaction_date"],
        ),
        "obt_customer_trading_activity": (obt_customer_trading_activity, None),
    }

    print("Gold transform completed")
    for table_name, (df, partition_by) in outputs.items():
        path = write_gold(table_name, df, partition_by=partition_by)
        print(f"[{table_name}] rows={df.count()} output={path}")

    spark.stop()


if __name__ == "__main__":
    run_gold_transform()
