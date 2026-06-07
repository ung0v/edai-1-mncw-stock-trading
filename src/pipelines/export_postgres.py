import os

from sqlalchemy import create_engine

from src.common.spark import get_spark_session, layer_path
from src.common.tables import GOLD_TABLES

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+psycopg2://postgres:postgres@postgres:5432/stock_dw")


def export_table(spark, engine, table_name: str):
    df = spark.read.format("delta").load(layer_path("gold", table_name)).toPandas()
    df.to_sql(table_name, engine, schema="public", if_exists="replace", index=False, chunksize=5000)
    print(f"[{table_name}] exported rows={len(df)}")


def main():
    spark = get_spark_session("export-postgres")
    engine = create_engine(DATABASE_URL)
    for table in GOLD_TABLES:
        export_table(spark, engine, table)
    print("PostgreSQL export completed")
    spark.stop()


if __name__ == "__main__":
    main()
