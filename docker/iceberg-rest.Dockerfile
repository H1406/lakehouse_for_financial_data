# Iceberg REST fixture + Postgres JDBC driver, so the JdbcCatalog lives in Postgres instead of SQLite.
# SQLite could not take concurrent commits (SQLITE_BUSY / SQLITE_BUSY_SNAPSHOT); see docs/DECISIONS.md.
FROM apache/iceberg-rest-fixture:1.10.1
ADD --chmod=644 https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.15/postgresql-42.7.15.jar \
    /usr/lib/iceberg-rest/postgresql.jar
CMD ["java", "-cp", "iceberg-rest-adapter.jar:postgresql.jar", "org.apache.iceberg.rest.RESTCatalogServer"]
