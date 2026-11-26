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

    # Agents: DeepSeek (OpenAI-compatible chat completions with tool calls and thinking mode)
    deepseek_api_key: SecretStr = SecretStr("")
    deepseek_base_url: str = "https://api.deepseek.com"
    deepseek_model: str = "deepseek-flash"
    deepseek_reasoning_effort: str = "high"
    agent_ro_password: SecretStr = SecretStr("agent_ro")
    agent_rw_password: SecretStr = SecretStr("agent_rw")
    airflow_api_url: str = "http://airflow:8080"
    pii_admin_password: SecretStr = SecretStr("pii_admin")
    grafana_ro_password: SecretStr = SecretStr("grafana_ro")
    pii_hmac_secret: SecretStr = SecretStr("change-me-laptop-only")

    kafka_bootstrap: str = "redpanda:9092"
    alert_webhook_url: str = ""
    data_dir: str = "/opt/lakehouse/data"

    @property
    def pg_dsn(self) -> str:
        pw = self.etl_writer_password.get_secret_value()
        return f"host={self.pg_host} port={self.pg_port} dbname={self.pg_db} user=etl_writer password={pw}"


    def dsn_for(self, role: str) -> str:
        """DSN for a named least-privilege role (agent_ro, agent_rw, pii_admin)."""
        pw = getattr(self, f"{role}_password").get_secret_value()
        return f"host={self.pg_host} port={self.pg_port} dbname={self.pg_db} user={role} password={pw}"


@lru_cache
def settings() -> Settings:
    return Settings()
