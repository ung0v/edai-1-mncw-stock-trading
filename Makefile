.PHONY: generate reports bronze silver gold features quality pipeline clean

network-up:
	docker network inspect data-stack-network >/dev/null 2>&1 || docker network create data-stack-network

generate:
	python -m src.generator.generate_offline
	python -m src.generator.generate_stream

reports:
	python -m src.generator.generate_reports

bronze:
	python -m src.pipelines.bronze_ingest

silver:
	python -m src.pipelines.silver_transform

gold:
	python -m src.pipelines.gold_transform

features:
	python -m src.pipelines.feature_transform

quality:
	python -m src.pipelines.quality_checks

postgres:
	python -m src.pipelines.export_postgres

indexes:
	docker exec -i stock_dw_postgres psql -U postgres -d stock_dw < src/pipelines/create_postgres_indexes.sql
airflow-init:
	$(MAKE) network-up
	docker compose up airflow-init

airflow-up:
	$(MAKE) network-up
	docker compose up -d airflow-webserver airflow-scheduler

airflow-down:
	docker compose down

datahub-up:
	$(MAKE) network-up
	docker compose -f docker-compose.yml -f data-hub-docker-compose.yaml up -d

datahub-down:
	docker compose -f docker-compose.yml -f data-hub-docker-compose.yaml stop datahub-frontend datahub-actions datahub-gms datahub-system-update opensearch datahub-postgres-setup

kafka-up:
	$(MAKE) network-up
	docker compose up -d kafka

kafka-produce:
	KAFKA_BOOTSTRAP_SERVERS=localhost:9092 python -m src.pipelines.kafka_producer

kafka-consume:
	KAFKA_BOOTSTRAP_SERVERS=localhost:9092 KAFKA_MAX_MESSAGES=1000 python -m src.pipelines.kafka_consumer_to_jsonl

pipeline: generate reports bronze silver gold features quality

clean:
	rm -rf data/raw
	rm -rf data/bronze
	rm -rf data/silver
	rm -rf data/gold
	rm -rf outputs
