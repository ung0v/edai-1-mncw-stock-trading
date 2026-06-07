from pyspark.sql import functions as F

from src.common.spark import get_spark_session, layer_path, write_delta


def read_gold(spark, table_name: str):
    return spark.read.format("delta").load(layer_path("gold", table_name))


def read_silver(spark, table_name: str):
    return spark.read.format("delta").load(layer_path("silver", f"stg_{table_name}"))


def write_gold(table_name: str, df):
    path = layer_path("gold", table_name)
    write_delta(df, path, mode="overwrite")
    return path


def build_feat_customer_90d(fact_order, fact_trade):
    order_ref = fact_order.agg(F.max("order_timestamp").alias("max_ts")).collect()[0]["max_ts"]
    trade_ref = fact_trade.agg(F.max("trade_timestamp").alias("max_ts")).collect()[0]["max_ts"]
    reference_ts = max(order_ref, trade_ref)

    orders_90d = fact_order.filter(F.col("order_timestamp").between(F.lit(reference_ts) - F.expr("INTERVAL 90 DAYS"), F.lit(reference_ts)))
    trades_90d = fact_trade.filter(F.col("trade_timestamp").between(F.lit(reference_ts) - F.expr("INTERVAL 90 DAYS"), F.lit(reference_ts)))

    order_features = orders_90d.groupBy("customer_id").agg(
        F.count("order_id").alias("f_customer_order_count_90d"),
        F.sum(F.when(F.col("order_side") == "BUY", 1).otherwise(0)).alias("f_customer_buy_order_count_90d"),
        F.sum(F.when(F.col("order_side") == "SELL", 1).otherwise(0)).alias("f_customer_sell_order_count_90d"),
        F.sum("order_amount").alias("f_customer_total_order_amount_90d"),
        F.avg("order_amount").alias("f_customer_avg_order_amount_90d"),
        F.countDistinct("ticker").alias("f_customer_distinct_ticker_90d"),
    )

    trade_features = trades_90d.groupBy("customer_id").agg(
        F.count("trade_id").alias("f_customer_trade_count_90d"),
        F.sum("trade_amount").alias("f_customer_total_trade_amount_90d"),
        F.sum("fee_amount").alias("f_customer_total_fee_amount_90d"),
    )

    return (
        order_features.join(trade_features, on="customer_id", how="outer")
        .fillna(0)
        .withColumn("event_timestamp", F.lit(reference_ts))
        .withColumn("created_ts", F.current_timestamp())
    )


def build_feat_security_1d(fact_order, fact_trade):
    order_ref = fact_order.agg(F.max("order_timestamp").alias("max_ts")).collect()[0]["max_ts"]
    trade_ref = fact_trade.agg(F.max("trade_timestamp").alias("max_ts")).collect()[0]["max_ts"]
    reference_ts = max(order_ref, trade_ref)

    orders_1d = fact_order.filter(F.col("order_timestamp").between(F.lit(reference_ts) - F.expr("INTERVAL 1 DAY"), F.lit(reference_ts)))
    trades_1d = fact_trade.filter(F.col("trade_timestamp").between(F.lit(reference_ts) - F.expr("INTERVAL 1 DAY"), F.lit(reference_ts)))

    order_features = orders_1d.groupBy("security_id").agg(
        F.count("order_id").alias("f_security_order_count_1d"),
        F.sum(F.when(F.col("order_side") == "BUY", 1).otherwise(0)).alias("f_security_buy_order_count_1d"),
        F.sum(F.when(F.col("order_side") == "SELL", 1).otherwise(0)).alias("f_security_sell_order_count_1d"),
        F.sum("order_amount").alias("f_security_total_order_amount_1d"),
        F.countDistinct("customer_id").alias("f_security_distinct_customer_1d"),
    )

    trade_features = trades_1d.groupBy("security_id").agg(
        F.count("trade_id").alias("f_security_trade_count_1d"),
        F.sum("trade_amount").alias("f_security_total_trade_amount_1d"),
        F.avg("trade_price").alias("f_security_avg_trade_price_1d"),
    )

    return (
        order_features.join(trade_features, on="security_id", how="outer")
        .fillna(0)
        .withColumn("event_timestamp", F.lit(reference_ts))
        .withColumn("created_ts", F.current_timestamp())
    )


def build_feat_stream_customer_60m(events):
    reference_ts = events.agg(F.max("event_timestamp").alias("max_ts")).collect()[0]["max_ts"]
    events_60m = events.filter(F.col("event_timestamp").between(F.lit(reference_ts) - F.expr("INTERVAL 60 MINUTES"), F.lit(reference_ts)))

    return events_60m.groupBy("customer_id").agg(
        F.count("event_id").alias("f_stream_event_count_60m"),
        F.sum(F.when(F.col("event_type") == "ORDER_PLACED", 1).otherwise(0)).alias("f_stream_order_placed_count_60m"),
        F.sum(F.when(F.col("event_type") == "ORDER_MATCHED", 1).otherwise(0)).alias("f_stream_order_matched_count_60m"),
        F.sum(F.when(F.col("event_type") == "PRICE_VIEWED", 1).otherwise(0)).alias("f_stream_price_viewed_count_60m"),
        F.countDistinct("ticker").alias("f_stream_distinct_ticker_60m"),
        F.sum(F.when(F.col("is_late_arrival"), 1).otherwise(0)).alias("f_stream_late_event_count_60m"),
    ).withColumn("event_timestamp", F.lit(reference_ts)).withColumn("created_ts", F.current_timestamp())


def build_feat_customer_unified(feat_customer_90d, feat_stream_customer_60m):
    offline = feat_customer_90d.select(
        "customer_id",
        *[column for column in feat_customer_90d.columns if column not in {"customer_id", "event_timestamp", "created_ts"}],
        F.col("event_timestamp").alias("offline_event_timestamp"),
    )
    stream = feat_stream_customer_60m.select(
        "customer_id",
        *[column for column in feat_stream_customer_60m.columns if column not in {"customer_id", "event_timestamp", "created_ts"}],
        F.col("event_timestamp").alias("stream_event_timestamp"),
    )

    numeric_cols = [
        column
        for column, dtype in offline.join(stream, on="customer_id", how="outer").dtypes
        if dtype in {"bigint", "int", "double", "float", "decimal", "smallint", "tinyint"}
    ]

    return (
        offline.join(stream, on="customer_id", how="outer")
        .fillna(0, subset=[column for column in numeric_cols if column != "customer_id"])
        .withColumn(
            "event_timestamp",
            F.coalesce(F.col("offline_event_timestamp"), F.col("stream_event_timestamp")),
        )
        .withColumn("created_ts", F.current_timestamp())
        .drop("offline_event_timestamp", "stream_event_timestamp")
    )


def run_feature_transform():
    spark = get_spark_session("feature-transform")

    fact_order = read_gold(spark, "fact_order")
    fact_trade = read_gold(spark, "fact_trade")
    events = read_silver(spark, "trading_events")

    feat_customer_90d = build_feat_customer_90d(fact_order, fact_trade)
    feat_security_1d = build_feat_security_1d(fact_order, fact_trade)
    feat_stream_customer_60m = build_feat_stream_customer_60m(events)
    feat_customer_unified = build_feat_customer_unified(feat_customer_90d, feat_stream_customer_60m)

    outputs = {
        "feat_customer_90d": feat_customer_90d,
        "feat_security_1d": feat_security_1d,
        "feat_stream_customer_60m": feat_stream_customer_60m,
        "feat_customer_unified": feat_customer_unified,
    }

    print("Feature transform completed")
    for table_name, df in outputs.items():
        path = write_gold(table_name, df)
        print(f"[{table_name}] rows={df.count()} output={path}")

    spark.stop()


if __name__ == "__main__":
    run_feature_transform()
