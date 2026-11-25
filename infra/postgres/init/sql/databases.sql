-- Databases and their owner roles (run against the `postgres` DB). Idempotent: `make init` re-runs it.
select format('create role %I login password %L', r.name, r.pw)
from (values ('airflow', 'airflow'), ('iceberg', :'iceberg_pw')) as r(name, pw)
where not exists (select 1 from pg_roles where rolname = r.name)
\gexec
select format('create database %I owner %I', d.name, d.owner)
from (values ('airflow', 'airflow'), ('iceberg_catalog', 'iceberg'), ('warehouse', 'postgres')) as d(name, owner)
where not exists (select 1 from pg_database where datname = d.name)
\gexec
