.PHONY: generate reports bronze silver gold features quality pipeline clean \
	clean-raw clean-local-lake clean-outputs clean-airflow-logs \
	reset-minio reset-postgres reset-kafka reset-data reset-all

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
	$(MAKE) clean-raw
	$(MAKE) clean-local-lake
	$(MAKE) clean-outputs

clean-raw:
	rm -rf data/raw
	rm -rf data/stream

clean-local-lake:
	rm -rf data/bronze
	rm -rf data/silver
	rm -rf data/gold

clean-outputs:
	rm -rf outputs

clean-airflow-logs:
	rm -rf airflow/logs/*

reset-minio:
	docker run --rm --network data-stack-network minio/mc /bin/sh -c '\
		mc alias set local http://minio:9000 minioadmin minioadmin && \
		mc rm -r --force local/stock-lakehouse/bronze || true && \
		mc rm -r --force local/stock-lakehouse/silver || true && \
		mc rm -r --force local/stock-lakehouse/gold || true && \
		mc rm -r --force local/stock-lakehouse/raw || true && \
		mc rm -r --force local/stock-lakehouse/checkpoints || true'

reset-postgres:
	docker exec stock_dw_postgres psql -U postgres -d stock_dw -c "DROP SCHEMA public CASCADE; CREATE SCHEMA public;"

reset-kafka:
	docker compose down
	docker volume rm $$(docker volume ls -q | grep 'kafka_data' || true)

reset-data:
	$(MAKE) clean
	$(MAKE) clean-airflow-logs
	$(MAKE) reset-minio
	$(MAKE) reset-postgres

reset-all:
	docker compose down -v --remove-orphans
	$(MAKE) clean
	$(MAKE) clean-airflow-logs
