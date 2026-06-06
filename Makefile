.PHONY: generate reports bronze silver gold features quality pipeline clean

generate:
	python src/generator/generate_offline.py
	python src/generator/generate_stream.py

reports:
	python src/generator/generate_reports.py

bronze:
	python src/pipelines/bronze_ingest.py

silver:
	python src/pipelines/silver_transform.py

gold:
	python src/pipelines/gold_transform.py

features:
	python src/pipelines/feature_transform.py

quality:
	python src/pipelines/quality_checks.py

postgres:
	python src/pipelines/export_postgres.py

indexes:
	docker exec -i stock_dw_postgres psql -U postgres -d stock_dw < src/pipelines/create_postgres_indexes.sql
airflow-init:
	docker compose up airflow-init

airflow-up:
	docker compose up -d airflow-webserver airflow-scheduler

airflow-down:
	docker compose down

kafka-up:
	docker compose up -d kafka

kafka-produce:
	KAFKA_BOOTSTRAP_SERVERS=localhost:9092 python src/pipelines/kafka_producer.py

kafka-consume:
	KAFKA_BOOTSTRAP_SERVERS=localhost:9092 KAFKA_MAX_MESSAGES=1000 python src/pipelines/kafka_consumer_to_jsonl.py

pipeline: generate reports bronze silver gold features quality

clean:
	rm -rf data/raw
	rm -rf data/bronze
	rm -rf data/silver
	rm -rf data/gold
	rm -rf outputs
