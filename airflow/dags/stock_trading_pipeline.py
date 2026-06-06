from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_DIR = "/opt/airflow/project"


with DAG(
    dag_id="stock_trading_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["coursework", "stock-trading"],
) as dag:
    generate_offline = BashOperator(
        task_id="generate_offline",
        bash_command=f"cd {PROJECT_DIR} && python src/generator/generate_offline.py",
    )

    generate_stream = BashOperator(
        task_id="generate_stream",
        bash_command=f"cd {PROJECT_DIR} && python src/generator/generate_stream.py",
    )

    produce_kafka_events = BashOperator(
        task_id="produce_kafka_events",
        bash_command=f"cd {PROJECT_DIR} && KAFKA_BOOTSTRAP_SERVERS=kafka:29092 python src/pipelines/kafka_producer.py",
    )

    consume_kafka_events = BashOperator(
        task_id="consume_kafka_events",
        bash_command=f"cd {PROJECT_DIR} && KAFKA_BOOTSTRAP_SERVERS=kafka:29092 python src/pipelines/kafka_consumer_to_jsonl.py",
    )

    reports = BashOperator(
        task_id="generate_reports",
        bash_command=f"cd {PROJECT_DIR} && python src/generator/generate_reports.py",
    )

    bronze = BashOperator(
        task_id="bronze_ingestion",
        bash_command=f"cd {PROJECT_DIR} && python src/pipelines/bronze_ingest.py",
    )

    silver = BashOperator(
        task_id="silver_transform",
        bash_command=f"cd {PROJECT_DIR} && python src/pipelines/silver_transform.py",
    )

    gold = BashOperator(
        task_id="gold_transform",
        bash_command=f"cd {PROJECT_DIR} && python src/pipelines/gold_transform.py",
    )

    features = BashOperator(
        task_id="feature_transform",
        bash_command=f"cd {PROJECT_DIR} && python src/pipelines/feature_transform.py",
    )

    quality = BashOperator(
        task_id="quality_checks",
        bash_command=f"cd {PROJECT_DIR} && python src/pipelines/quality_checks.py",
    )

    export_postgres = BashOperator(
        task_id="export_postgres",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            "DATABASE_URL=postgresql+psycopg2://postgres:postgres@postgres:5432/stock_dw "
            "python src/pipelines/export_postgres.py"
        ),
    )

    (
        generate_offline
        >> generate_stream
        >> produce_kafka_events
        >> consume_kafka_events
        >> reports
        >> bronze
        >> silver
        >> gold
        >> features
        >> quality
        >> export_postgres
    )
