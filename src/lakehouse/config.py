"""Settings from env (see .env.example). Defaults are the in-container hostnames."""

from functools import lru_cache

from pydantic import SecretStr
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    sec_user_agent: str = ""

    s3_endpoint: str = "http://seaweedfs:8333"
    s3_access_key: str = "lakehouse"
    s3_secret_key: SecretStr = SecretStr("lakehouse-secret")
    s3_region: str = "us-east-1"
    iceberg_rest_uri: str = "http://iceberg-rest:8181"
    warehouse: str = "s3://lake/warehouse"

    pg_host: str = "postgres"
    pg_port: int = 5432
    pg_db: str = "warehouse"
    etl_writer_password: SecretStr = SecretStr("etl_writer")

    kafka_bootstrap: str = "redpanda:9092"
    alert_webhook_url: str = ""
    data_dir: str = "/opt/lakehouse/data"

    @property
    def pg_dsn(self) -> str:
        pw = self.etl_writer_password.get_secret_value()
        return f"host={self.pg_host} port={self.pg_port} dbname={self.pg_db} user=etl_writer password={pw}"


@lru_cache
def settings() -> Settings:
    return Settings()
