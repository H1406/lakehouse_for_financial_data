# Pinned versions

Verified on 2026-11-25 against PyPI, Maven Central and Docker Hub. Change a pin only after checking the
registry again, and keep the Spark/Iceberg/Kafka rules below.

## Images

| Component | Image / tag | Notes |
|---|---|---|
| Object storage | `chrislusf/seaweedfs:4.55` | `weed server -s3`; needs `-ip.bind=0.0.0.0` for the healthcheck |
| Iceberg REST catalog | `apache/iceberg-rest-fixture:1.10.1` | Newest tag published. The server version does not need to match the 1.12 clients. |
| Broker | `redpandadata/redpanda:v26.3.1` | single node, `--mode dev-container` |
| Warehouse | `pgvector/pgvector:0.8.7-pg16` | PostgreSQL 16 + pgvector 0.8.7 |
| Airflow base | `apache/airflow:3.3.2-python3.12` | Debian bookworm, + `openjdk-17-jre-headless` (17.0.21) |
| Spark stream base | `apache/spark:4.0.5-scala2.13-java17-python3-ubuntu` | **Python 3.10** inside: code it imports must not use 3.11+ features |
| Producer base | `python:3.12-slim` | |

## JARs (baked into images, never `--packages`)

| JAR | Version | Image |
|---|---|---|
| `org.apache.iceberg:iceberg-spark-runtime-4.0_2.13` | 1.12.0 | airflow, spark-stream |
| `org.apache.iceberg:iceberg-aws-bundle` | 1.12.0 (ships AWS SDK v2 2.54.17) | airflow, spark-stream |
| `org.apache.spark:spark-sql-kafka-0-10_2.13` | 4.0.5 (= Spark version) | spark-stream |
| `org.apache.spark:spark-token-provider-kafka-0-10_2.13` | 4.0.5 | spark-stream |
| `org.apache.kafka:kafka-clients` | 3.9.1 (from the Spark 4.0.5 POM) | spark-stream |
| `org.apache.commons:commons-pool2` | 2.12.0 (from the Spark 4.0.5 POM) | spark-stream |
| `org.apache.hadoop:hadoop-aws` | 3.4.1 (= Spark 4.0.5's Hadoop) | spark-stream (s3a checkpoints) |
| `software.amazon.awssdk:s3-transfer-manager` | 2.54.17 (= bundle SDK) | spark-stream |
| `software.amazon.awssdk:apache-client` | 2.54.17 (= bundle SDK) | spark-stream |

Rules: the Iceberg runtime must match the Spark **minor** version and Scala 2.13. The Kafka connector must
equal the Spark version. Iceberg 1.12 ships runtimes for Spark 3.5, 4.0 and 4.1 only, so Spark stays on
4.0.x even though pyspark 4.3 is on PyPI.

## Python

| Package | Version | Where |
|---|---|---|
| Python | 3.12 (Airflow, producer, local dev); 3.10 (Spark stream image) | |
| apache-airflow | 3.3.2 (latest 3.3.x; 3.4.0 exists, plan targets 3.3) | airflow, with `constraints-3.3.2/constraints-3.12.txt` |
| apache-airflow-providers-standard | 1.19.0 (from constraints) | airflow |
| pyspark | 4.0.5 | airflow, dev |
| pyiceberg[pyarrow] | 0.12.0 | airflow, dev |
| pyarrow | 25.0.1 (from constraints) | airflow |
| pydantic / pydantic-settings | 2.13.5 / 2.15.0 | all |
| psycopg[binary] | 3.3.5 | all |
| requests / tenacity | 2.34.2 / 9.1.4 | airflow, dev |
| yfinance | 1.8.0 (pulls curl-cffi ≥ 0.17) | airflow, producer image, dev |
| certifi | unpinned in the Airflow image (constraints pin 2026.7.22 is dropped; curl-cffi needs ≥ 2026.10.19) | airflow |
| confluent-kafka / websockets | 2.15.1 / 16.1.1 | producer image (both feeds), dev |
| dbt-core / dbt-postgres | 1.12.7 / 1.11.0 | separate venv in the airflow image (`~/dbt-venv`) |
| dbt_utils | 1.4.1 | dbt package |
| pytest / ruff | 9.2.0 / 0.17.6 | dev |
