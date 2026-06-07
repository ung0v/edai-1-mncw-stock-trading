from pyspark.sql import functions as F

from src.common.spark import dedup_latest, get_spark_session, layer_path, write_delta
from src.common.tables import BRONZE_TABLES


TIMESTAMP_COLUMNS = {
    "customers": ["signup_ts", "created_ts", "updated_ts"],
    "accounts": ["opened_ts", "closed_ts", "created_ts", "updated_ts"],
    "securities": ["listed_date", "created_ts", "updated_ts"],
    "orders": ["order_timestamp", "created_ts", "updated_ts"],
    "trades": ["trade_timestamp", "created_ts"],
    "cash_transactions": ["transaction_timestamp", "created_ts"],
    "trading_events": ["event_timestamp", "created_ts"],
}


def read_bronze_table(spark, table_name: str):
    return spark.read.format("delta").load(layer_path("bronze", table_name))


def standardize_timestamps(df, columns: list[str]):
    for column in columns:
        if column in df.columns:
            df = df.withColumn(column, F.to_timestamp(F.col(column)))
    return df


def add_record_date(df):
    timestamp_sources = [column for column in ["updated_ts", "created_ts"] if column in df.columns]
    if not timestamp_sources:
        return df, None

    return df.withColumn("record_date", F.to_date(F.coalesce(*[F.col(column) for column in timestamp_sources]))), ["record_date"]


def clean_customers(df):
    return dedup_latest(standardize_timestamps(df, TIMESTAMP_COLUMNS["customers"]), ["customer_id"], "updated_ts")


def clean_accounts(df):
    return dedup_latest(standardize_timestamps(df, TIMESTAMP_COLUMNS["accounts"]), ["account_id"], "updated_ts")


def clean_securities(df):
    return dedup_latest(standardize_timestamps(df, TIMESTAMP_COLUMNS["securities"]), ["security_id"], "updated_ts")


def clean_orders(df):
    if "limit_price" not in df.columns:
        df = df.withColumn("limit_price", F.lit(None).cast("double"))

    df = standardize_timestamps(df, TIMESTAMP_COLUMNS["orders"])
    df = df.withColumn("order_quantity", F.col("order_quantity").cast("double"))
    df = df.withColumn("limit_price", F.col("limit_price").cast("double"))
    df = df.filter(
        F.col("order_id").isNotNull()
        & F.col("account_id").isNotNull()
        & F.col("customer_id").isNotNull()
        & F.col("security_id").isNotNull()
        & F.col("order_timestamp").isNotNull()
    )
    return dedup_latest(df, ["order_id"], "created_ts")


def clean_trades(df):
    df = standardize_timestamps(df, TIMESTAMP_COLUMNS["trades"])
    for column in ["trade_quantity", "trade_price", "trade_amount", "fee_amount"]:
        df = df.withColumn(column, F.col(column).cast("double"))
    df = df.filter(
        F.col("trade_id").isNotNull()
        & F.col("order_id").isNotNull()
        & F.col("trade_timestamp").isNotNull()
    )
    return dedup_latest(df, ["trade_id"], "created_ts")


def clean_cash_transactions(df):
    df = standardize_timestamps(df, TIMESTAMP_COLUMNS["cash_transactions"])
    df = df.withColumn("amount", F.col("amount").cast("double"))
    df = df.filter(
        F.col("cash_transaction_id").isNotNull()
        & F.col("account_id").isNotNull()
        & F.col("customer_id").isNotNull()
        & F.col("transaction_timestamp").isNotNull()
    )
    return dedup_latest(df, ["cash_transaction_id"], "created_ts")


def clean_trading_events(df):
    df = standardize_timestamps(df, TIMESTAMP_COLUMNS["trading_events"])
    df = df.filter(
        F.col("event_id").isNotNull()
        & F.col("account_id").isNotNull()
        & F.col("customer_id").isNotNull()
        & F.col("event_timestamp").isNotNull()
        & F.col("created_ts").isNotNull()
    )
    df = dedup_latest(df, ["event_id"], "created_ts")
    return (
        df.withColumn("is_late_arrival", F.col("created_ts") > F.col("event_timestamp"))
        .withColumn("arrival_delay_seconds", F.col("created_ts").cast("long") - F.col("event_timestamp").cast("long"))
    )


CLEANERS = {
    "customers": clean_customers,
    "accounts": clean_accounts,
    "securities": clean_securities,
    "orders": clean_orders,
    "trades": clean_trades,
    "cash_transactions": clean_cash_transactions,
    "trading_events": clean_trading_events,
}


def run_silver_transform():
    spark = get_spark_session("silver-transform")
    results = []

    for table in BRONZE_TABLES:
        bronze_df = read_bronze_table(spark, table)
        silver_df = CLEANERS[table](bronze_df)
        output_path = layer_path("silver", f"stg_{table}")
        partition_by = ["event_date"] if table == "trading_events" else None

        if table == "trading_events":
            silver_df = silver_df.withColumn("event_date", F.to_date("event_timestamp"))
        elif "created_ts" in silver_df.columns:
            silver_df, partition_by = add_record_date(silver_df)

        write_delta(silver_df, output_path, mode="overwrite", partition_by=partition_by)
        results.append(
            {
                "table": table,
                "bronze_rows": bronze_df.count(),
                "silver_rows": silver_df.count(),
                "output_path": output_path,
            }
        )

    print("Silver transform completed")
    for result in results:
        print(
            f"[{result['table']}] bronze={result['bronze_rows']} "
            f"silver={result['silver_rows']} output={result['output_path']}"
        )

    spark.stop()


if __name__ == "__main__":
    run_silver_transform()
