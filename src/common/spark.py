import os
import shutil
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.common.config import load_config
from src.common.paths import CONFIG_PATH, get_storage_uri

LOCAL_SPARK_JAR_DIR = Path("/opt/spark-extra-jars")
LOCAL_SPARK_JAR_NAMES = [
    "delta-spark_2.12-3.2.0.jar",
    "delta-storage-3.2.0.jar",
    "antlr4-runtime-4.9.3.jar",
    "hadoop-aws-3.3.4.jar",
    "aws-java-sdk-bundle-1.12.262.jar",
    "wildfly-openssl-1.0.7.Final.jar",
]


def ensure_java_env():
    candidate_paths = [
        os.environ.get("JAVA_HOME"),
        "/usr/lib/jvm/java-17-openjdk-amd64",
        "/usr/lib/jvm/java-17-openjdk",
        "/usr/lib/jvm/java-17-openjdk-arm64",
        "/usr/lib/jvm/java-1.17.0-openjdk-arm64",
        "/opt/java/openjdk",
    ]

    for candidate in candidate_paths:
        if not candidate:
            continue

        java_bin = Path(candidate) / "bin" / "java"
        if java_bin.exists():
            os.environ["JAVA_HOME"] = str(candidate)
            os.environ["PATH"] = f"{candidate}/bin:{os.environ.get('PATH', '')}"
            return

    java_bin = shutil.which("java")
    if java_bin:
        resolved_java = Path(java_bin).resolve()
        java_home = resolved_java.parents[1]
        os.environ["JAVA_HOME"] = str(java_home)
        os.environ["PATH"] = f"{java_home}/bin:{os.environ.get('PATH', '')}"
        return

    raise RuntimeError(
        "JAVA_HOME is not configured and no Java runtime was found for Spark."
    )


def get_local_spark_jars(jar_dir: Path = LOCAL_SPARK_JAR_DIR) -> list[str]:
    jars = [jar_dir / jar_name for jar_name in LOCAL_SPARK_JAR_NAMES]
    if all(jar.exists() for jar in jars):
        return [str(jar) for jar in jars]
    return []


def get_spark_session(app_name: str | None = None) -> SparkSession:
    ensure_java_env()
    config = load_config(CONFIG_PATH)
    spark_cfg = config["spark"]
    storage_cfg = config["storage"]
    minio_cfg = storage_cfg["minio"]

    builder = (
        SparkSession.builder.appName(app_name or spark_cfg["app_name"])
        .master(spark_cfg.get("master", "local[*]"))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config(
            "spark.sql.shuffle.partitions",
            str(spark_cfg.get("shuffle_partitions", 8)),
        )
        .config("spark.hadoop.fs.s3a.endpoint", minio_cfg["endpoint"])
        .config("spark.hadoop.fs.s3a.access.key", minio_cfg["access_key"])
        .config("spark.hadoop.fs.s3a.secret.key", minio_cfg["secret_key"])
        .config(
            "spark.hadoop.fs.s3a.path.style.access",
            str(minio_cfg.get("path_style_access", True)).lower(),
        )
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.connection.ssl.enabled",
            str(minio_cfg["endpoint"].startswith("https://")).lower(),
        )
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
    )

    local_jars = get_local_spark_jars()
    if local_jars:
        spark = builder.config("spark.jars", ",".join(local_jars)).getOrCreate()
    else:
        from delta import configure_spark_with_delta_pip

        extra_packages = [
            package
            for package in spark_cfg["jars_packages"]
            if not package.startswith("io.delta:delta-spark_")
        ]
        spark = configure_spark_with_delta_pip(
            builder,
            extra_packages=extra_packages,
        ).getOrCreate()
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))
    return spark


def write_delta(df, path: str, mode: str = "overwrite", partition_by: list[str] | None = None):
    writer = df.write.format("delta").mode(mode)
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(path)


def dedup_latest(df, key_columns: list[str], order_column: str):
    window = Window.partitionBy(*key_columns).orderBy(F.col(order_column).desc_nulls_last())
    return (
        df.withColumn("_row_num", F.row_number().over(window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )


def add_ingest_metadata(df, source_file_col: str = "_source_file"):
    return (
        df.withColumn("ingest_ts", F.current_timestamp())
        .withColumn("source_file", F.col(source_file_col))
    )


def layer_path(layer: str, table_name: str) -> str:
    return get_storage_uri(layer, table_name)
