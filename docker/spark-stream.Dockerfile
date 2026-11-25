# Long-running Spark Structured Streaming service (Kafka -> Iceberg).
# hadoop-aws + s3-transfer-manager + apache-client: s3a:// for checkpoints; the AWS SDK itself comes from iceberg-aws-bundle (2.54.17).
FROM apache/spark:4.0.5-scala2.13-java17-python3-ubuntu

USER root
ARG MVN=https://repo1.maven.org/maven2
RUN cd /opt/spark/jars \
 && curl -fsSLO $MVN/org/apache/iceberg/iceberg-spark-runtime-4.0_2.13/1.12.0/iceberg-spark-runtime-4.0_2.13-1.12.0.jar \
 && curl -fsSLO $MVN/org/apache/iceberg/iceberg-aws-bundle/1.12.0/iceberg-aws-bundle-1.12.0.jar \
 && curl -fsSLO $MVN/org/apache/spark/spark-sql-kafka-0-10_2.13/4.0.5/spark-sql-kafka-0-10_2.13-4.0.5.jar \
 && curl -fsSLO $MVN/org/apache/spark/spark-token-provider-kafka-0-10_2.13/4.0.5/spark-token-provider-kafka-0-10_2.13-4.0.5.jar \
 && curl -fsSLO $MVN/org/apache/kafka/kafka-clients/3.9.1/kafka-clients-3.9.1.jar \
 && curl -fsSLO $MVN/org/apache/commons/commons-pool2/2.12.0/commons-pool2-2.12.0.jar \
 && curl -fsSLO $MVN/org/apache/hadoop/hadoop-aws/3.4.1/hadoop-aws-3.4.1.jar \
 && curl -fsSLO $MVN/software/amazon/awssdk/s3-transfer-manager/2.54.17/s3-transfer-manager-2.54.17.jar \
 && curl -fsSLO $MVN/software/amazon/awssdk/apache-client/2.54.17/apache-client-2.54.17.jar
RUN pip install --no-cache-dir "psycopg[binary]==3.3.5" "pydantic-settings==2.15.0"

USER spark
ENV PYTHONPATH=/opt/lakehouse/src:/opt/lakehouse
WORKDIR /opt/lakehouse
CMD ["/opt/spark/bin/spark-submit", "--master", "local[2]", "--driver-memory", "1g", "spark_jobs/stream_market.py"]
