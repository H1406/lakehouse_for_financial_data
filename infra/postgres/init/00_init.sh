#!/bin/bash
# Runs once on first start of an empty volume. `make init` re-applies the same SQL (all statements are idempotent).
set -euo pipefail
psql -v ON_ERROR_STOP=1 -U postgres -d postgres -v iceberg_pw="$ICEBERG_CATALOG_PASSWORD" \
  -f /docker-entrypoint-initdb.d/sql/databases.sql
psql -v ON_ERROR_STOP=1 -U postgres -d warehouse \
  -v etl_writer_pw="$ETL_WRITER_PASSWORD" -v dbt_pw="$DBT_PASSWORD" \
  -v grafana_ro_pw="$GRAFANA_RO_PASSWORD" -v agent_ro_pw="$AGENT_RO_PASSWORD" -v agent_rw_pw="$AGENT_RW_PASSWORD" -v pii_admin_pw="$PII_ADMIN_PASSWORD" \
  -f /docker-entrypoint-initdb.d/sql/warehouse.sql
