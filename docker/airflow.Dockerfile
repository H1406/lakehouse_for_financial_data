# Airflow 3 + JDK 17 + pyspark with Iceberg JARs baked in (no --packages at runtime).
FROM apache/airflow:3.3.2-python3.12

USER root
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl \
    && rm -rf /var/lib/apt/lists/* \
    && ln -s "$(dirname "$(dirname "$(readlink -f "$(which java)")")")" /usr/lib/jvm/java-17
ENV JAVA_HOME=/usr/lib/jvm/java-17

USER airflow
COPY docker/requirements-airflow.txt /tmp/requirements.txt
# Airflow's constraints file minus its certifi pin: yfinance -> curl-cffi 0.17 needs certifi >= 2026.10.19.
# certifi is only a CA bundle, so a newer one is safe; every other constrained package keeps its pin.
RUN curl -fsSL https://raw.githubusercontent.com/apache/airflow/constraints-3.3.2/constraints-3.12.txt \
      | grep -v '^certifi==' > /tmp/constraints.txt \
 && pip install --no-cache-dir "apache-airflow==3.3.2" -r /tmp/requirements.txt --constraint /tmp/constraints.txt

# Bake JARs into pyspark's own jars dir so every SparkSession sees them.
ARG MVN=https://repo1.maven.org/maven2
RUN JARS="$(python -c 'import pyspark,os;print(os.path.join(os.path.dirname(pyspark.__file__),"jars"))')" \
 && curl -fsSL -o "$JARS/iceberg-spark-runtime-4.0_2.13-1.12.0.jar" $MVN/org/apache/iceberg/iceberg-spark-runtime-4.0_2.13/1.12.0/iceberg-spark-runtime-4.0_2.13-1.12.0.jar \
 && curl -fsSL -o "$JARS/iceberg-aws-bundle-1.12.0.jar" $MVN/org/apache/iceberg/iceberg-aws-bundle/1.12.0/iceberg-aws-bundle-1.12.0.jar

# dbt lives in its own venv: its dependency pins conflict with Airflow's constraints file.
RUN python -m venv /home/airflow/dbt-venv \
 && /home/airflow/dbt-venv/bin/pip install --no-cache-dir dbt-core==1.12.7 dbt-postgres==1.11.0
ENV PATH="${PATH}:/home/airflow/dbt-venv/bin" \
    DBT_PROFILES_DIR=/opt/lakehouse/dbt
WORKDIR /opt/lakehouse
