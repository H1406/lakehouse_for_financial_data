-- Warehouse DB: roles, schemas, ops telemetry and lake_silver copy targets.
-- Idempotent: safe to re-run from `make init`. Passwords come in as psql variables (from env).

create extension if not exists vector;

-- Roles ------------------------------------------------------------------
select format('create role %I login password %L', r.name, r.pw)
from (values ('etl_writer', :'etl_writer_pw'), ('dbt', :'dbt_pw'), ('grafana_ro', :'grafana_ro_pw'),
             ('agent_ro', :'agent_ro_pw'), ('agent_rw', :'agent_rw_pw'), ('pii_admin', :'pii_admin_pw')) as r(name, pw)
where not exists (select 1 from pg_roles where rolname = r.name)
\gexec

-- Schemas ----------------------------------------------------------------
create schema if not exists ops authorization etl_writer;
create schema if not exists lake_silver authorization etl_writer;
create schema if not exists staging authorization dbt;
create schema if not exists intermediate authorization dbt;
create schema if not exists marts authorization dbt;
create schema if not exists agent authorization agent_rw;
create schema if not exists pii authorization pii_admin;

-- ops: pipeline telemetry --------------------------------------------------
set role etl_writer;

create table if not exists ops.pipeline_run (
    run_id                 text primary key,           -- one row per task invocation (uuid)
    airflow_run_id         text,                       -- Airflow dag_run id, for log lookups
    dag_id                 text,
    task_id                text,
    source                 text,
    entity                 text,
    logical_date           date,
    started_at             timestamptz not null,
    finished_at            timestamptz,
    status                 text not null check (status in ('running', 'success', 'failed')),
    rows_read              bigint,
    rows_written           bigint,
    rows_quarantined       bigint,
    error_class            text,
    error_message_scrubbed text
);
create index if not exists pipeline_run_dag_started on ops.pipeline_run (dag_id, started_at desc);

create table if not exists ops.watermark (
    source          text not null,
    entity          text not null,
    watermark_value text not null,
    updated_at      timestamptz not null default now(),
    primary key (source, entity)
);

create table if not exists ops.reconciliation (
    run_id       text not null,
    source       text not null,
    entity       text not null,
    logical_date date,
    layer_from   text not null,
    layer_to     text not null,
    count_from   bigint not null,
    count_to     bigint not null,
    missing      bigint not null default 0,
    duplicates   bigint not null default 0,
    late         bigint not null default 0,
    status       text not null check (status in ('ok', 'mismatch')),
    checked_at   timestamptz not null default now()
);
create index if not exists reconciliation_checked on ops.reconciliation (checked_at desc);

create table if not exists ops.stream_metrics (
    batch_id              bigint not null,
    query_name            text not null,
    window_start          timestamptz,
    topic                 text not null,
    events                bigint not null,
    late_events           bigint not null,
    parse_errors          bigint not null,
    max_event_lag_seconds double precision,
    written_at            timestamptz not null default now()
);
create index if not exists stream_metrics_written on ops.stream_metrics (written_at desc);

create table if not exists ops.incident (
    incident_id       bigserial primary key,
    detected_at       timestamptz not null default now(),
    alert_name        text not null,
    severity          text not null,
    dag_id            text,
    airflow_run_id    text,
    task_id           text,
    triage_note_path  text,
    agent_diagnosis   text,
    human_verdict     text,
    resolved_at       timestamptz
);

-- At most one OPEN incident per failed DAG run (dedupes mapped tasks failing together; atomic under concurrency).
create unique index if not exists incident_open_per_run on ops.incident (dag_id, airflow_run_id)
    where resolved_at is null and airflow_run_id is not null;

create table if not exists ops.dbt_result (
    invocation_id          text not null,
    unique_id              text not null,
    resource_type          text not null,
    status                 text not null,
    failures               bigint,
    execution_time_s       double precision,
    message_scrubbed       text,
    generated_at           timestamptz not null,
    primary key (invocation_id, unique_id)
);

-- lake_silver: copies of Iceberg Silver tables (upsert by key) ------------
create table if not exists lake_silver.prices_eod (
    ticker        text not null,
    trade_date    date not null,
    open          double precision,
    high          double precision,
    low           double precision,
    close         double precision,
    adj_close     double precision,
    volume        bigint,
    div_cash      double precision,
    split_factor  double precision,
    _run_id       text,
    _source       text,
    _ingested_at  timestamptz,
    _payload_hash text,
    _updated_at   timestamptz not null,
    primary key (ticker, trade_date)
);


create table if not exists lake_silver.equity_bars_1m (
    ticker        text not null,
    minute        timestamptz not null,
    open          double precision,
    high          double precision,
    low           double precision,
    close         double precision,
    volume        bigint,
    published_at  timestamptz,        -- when the delayed feed released the bar (minute + >= 10 min)
    _run_id       text,
    _source       text,
    _ingested_at  timestamptz,
    _payload_hash text,
    _updated_at   timestamptz not null,
    primary key (ticker, minute)
);

create table if not exists lake_silver.financial_facts (
    cik           text not null,
    ticker        text,
    taxonomy      text not null,
    concept       text not null,
    unit          text not null,
    period_start  date,                 -- null for instant facts (balance-sheet items)
    period_end    date not null,
    value         double precision,
    accession_no  text not null,
    fy            integer,
    fp            text,
    form          text,
    filed_date    date,
    frame         text,
    _run_id       text,
    _source       text,
    _ingested_at  timestamptz,
    _payload_hash text,
    _updated_at   timestamptz not null,
    constraint financial_facts_key unique nulls not distinct
        (cik, concept, unit, period_start, period_end, accession_no)
);

create table if not exists lake_silver.crypto_bars_1m (
    symbol       text not null,
    minute       timestamptz not null,
    open         double precision,
    high         double precision,
    low          double precision,
    close        double precision,
    volume       double precision,
    trade_count  bigint,
    vwap         double precision,
    _updated_at  timestamptz not null,
    primary key (symbol, minute)
);

reset role;

-- One-time migration Tiingo -> yfinance (yfinance has no adjusted open/high/low/volume), run as superuser
-- because it touches a dbt-owned view. Runs only while the
-- old column exists. The dbt view selecting those columns is dropped explicitly (not CASCADE); dbt recreates it.
do $$
begin
    if exists (select 1 from information_schema.columns
               where table_schema = 'lake_silver' and table_name = 'prices_eod' and column_name = 'adj_open') then
        drop view if exists staging.stg_prices_eod;
        alter table lake_silver.prices_eod
            drop column adj_open, drop column adj_high, drop column adj_low, drop column adj_volume;
    end if;
end $$;

-- Grants ------------------------------------------------------------------
grant usage on schema lake_silver, ops to dbt;
grant select on all tables in schema lake_silver, ops to dbt;
alter default privileges for role etl_writer in schema lake_silver grant select on tables to dbt;

grant usage on schema ops to grafana_ro, agent_ro;
grant select on all tables in schema ops to grafana_ro, agent_ro;
alter default privileges for role etl_writer in schema ops grant select on tables to grafana_ro, agent_ro, dbt;

grant usage on schema marts to grafana_ro, agent_ro;
alter default privileges for role dbt in schema marts grant select on tables to grafana_ro, agent_ro;

-- agent: retrieval store for the QA and triage agents (owned by agent_rw) ---
set role agent_rw;

create table if not exists agent.documents (
    doc_id        text primary key,               -- e.g. 'NVDA-2026-Q3', 'runbook:stream_stalled'
    kind          text not null check (kind in ('fact_sheet', 'runbook')),
    ticker        text,
    fiscal_year   integer,
    fiscal_period text,
    title         text not null,
    path          text not null,
    content_hash  text not null,                  -- re-embed only when this changes
    metadata      jsonb not null default '{}',
    updated_at    timestamptz not null default now()
);

create table if not exists agent.chunks (
    chunk_id   text primary key,                  -- '<doc_id>#<n>'
    doc_id     text not null references agent.documents (doc_id) on delete cascade,
    chunk_no   integer not null,
    section    text,
    content    text not null,
    embedding  vector(384) not null,              -- all-MiniLM-L6-v2, normalized
    tsv        tsvector generated always as (to_tsvector('english', content)) stored
);
create index if not exists chunks_embedding_hnsw on agent.chunks using hnsw (embedding vector_cosine_ops);
create index if not exists chunks_tsv_gin on agent.chunks using gin (tsv);
create index if not exists chunks_doc on agent.chunks (doc_id);

-- Every prompt, model reply and tool call of every agent run (guardrail requirement: runs are auditable).
create table if not exists agent.run_log (
    id         bigserial primary key,
    run_id     text not null,
    agent      text not null,
    step       integer,
    event      text not null,                     -- start | llm | tool | guardrail | final
    tool_name  text,
    payload    jsonb,
    created_at timestamptz not null default now()
);
create index if not exists run_log_run on agent.run_log (run_id, id);

reset role;

-- PII (stretch): contact details live only in `pii`; analytics sees an HMAC key -----------------------
set role pii_admin;
create table if not exists pii.subscribers (
    subscriber_id bigint primary key,
    name          text not null,
    email         text not null,
    phone         text not null,
    created_at    timestamptz not null default now()
);
reset role;

set role etl_writer;
create table if not exists lake_silver.alert_subscriptions (
    subscriber_key text not null,                 -- HMAC-SHA256(secret, subscriber_id): no PII
    ticker         text not null,
    z_threshold    double precision not null default 3,
    primary key (subscriber_key, ticker)
);
reset role;

-- Grants for the agents and PII -----------------------------------------------------------------------
grant usage on schema agent to agent_ro, dbt;
grant select on agent.documents, agent.chunks to agent_ro;
grant select on all tables in schema agent to dbt;    -- read-only, for the data browser

-- Triage agent: may read ops and write ONLY its two columns of ops.incident (column-level grant).
grant usage on schema ops to agent_rw;
grant select on all tables in schema ops to agent_rw;
grant update (agent_diagnosis, triage_note_path) on ops.incident to agent_rw;

revoke all on schema pii from public;
grant usage on schema marts to pii_admin;
alter default privileges for role dbt in schema marts grant select on tables to pii_admin;
grant select on all tables in schema marts to pii_admin;
