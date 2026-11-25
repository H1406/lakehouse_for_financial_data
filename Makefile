COMPOSE := docker compose -f infra/docker-compose.yml --env-file .env
AIRFLOW := $(COMPOSE) exec -T airflow
PY := .venv/bin/python

.PHONY: up up-core down build init backfill stream-start stream-stop dbt-build test test-int lint smoke fixtures browse
JAVA_HOME ?= /opt/homebrew/opt/openjdk@17

.env:
	cp .env.example .env && echo "Created .env from .env.example: fill in SEC_USER_AGENT"

build: .env
	$(COMPOSE) --profile stream build

up: .env
	$(COMPOSE) --profile stream up -d

up-core: .env
	$(COMPOSE) up -d

down:
	$(COMPOSE) --profile stream down

init:
	$(COMPOSE) exec -T seaweedfs sh -c "echo 's3.bucket.create -name lake' | weed shell" || true
	$(COMPOSE) exec -T postgres sh -c 'psql -v ON_ERROR_STOP=1 -U postgres -d postgres -v iceberg_pw="$$ICEBERG_CATALOG_PASSWORD" \
	  -f /docker-entrypoint-initdb.d/sql/databases.sql'
	$(COMPOSE) exec -T postgres sh -c 'psql -v ON_ERROR_STOP=1 -U postgres -d warehouse \
	  -v etl_writer_pw="$$ETL_WRITER_PASSWORD" -v dbt_pw="$$DBT_PASSWORD" -v grafana_ro_pw="$$GRAFANA_RO_PASSWORD" \
	  -v agent_ro_pw="$$AGENT_RO_PASSWORD" -v agent_rw_pw="$$AGENT_RW_PASSWORD" \
	  -f /docker-entrypoint-initdb.d/sql/warehouse.sql'
	$(AIRFLOW) python scripts/init_lake.py
	$(AIRFLOW) bash -c "cd dbt && dbt deps && dbt seed --full-refresh"  # seeds are tiny; full refresh handles column changes

fixtures:  ## record real API responses into tests/fixtures (needs SEC_USER_AGENT in .env)
	$(PY) scripts/record_fixtures.py

browse:  ## read-only web GUI over Postgres, Iceberg tables and files
	$(COMPOSE) up -d data-browser
	@echo "open http://localhost:8501"

smoke:  ## Iceberg round trip: PyIceberg create -> Spark append -> PyIceberg read
	$(AIRFLOW) python -m pytest -q tests/integration/test_iceberg_roundtrip.py

backfill:  ## dates use macOS `date -v`; pass START=/END= elsewhere
	$(AIRFLOW) airflow dags unpause backfill_prices
	$(AIRFLOW) airflow dags unpause sec_facts_weekly
	$(AIRFLOW) airflow dags trigger backfill_prices -c '{"start": "$(or $(START),$(shell date -v-5y +%F))", "end": "$(or $(END),$(shell date -v-1d +%F))"}'
	$(AIRFLOW) airflow dags trigger sec_facts_weekly

stream-start:
	$(COMPOSE) --profile stream up -d redpanda producer equity-producer spark-stream

stream-stop:  ## stops only the Spark job (the RAM user); the producer keeps buffering into Redpanda, so no gap
	$(COMPOSE) --profile stream stop spark-stream

dbt-build:
	$(AIRFLOW) bash -c "cd dbt && dbt source freshness; dbt build"

test: lint
	JAVA_HOME=$(JAVA_HOME) $(PY) -m pytest -q tests/unit

test-int:
	$(AIRFLOW) python -m pytest -q tests/integration

lint:
	.venv/bin/ruff check src spark_jobs dags scripts streaming tests
