import os

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # Pub/Sub
    pubsub_project_id: str = "local-project"
    pubsub_topic_id: str = "cdc-events"
    pubsub_subscription_id: str = "cdc-pipeline-sub"
    # When set, the Google Pub/Sub client uses the emulator automatically
    # via os.environ["PUBSUB_EMULATOR_HOST"]. We sync it below.
    pubsub_emulator_host: str = ""

    # GCS
    gcs_bucket_name: str = "pdf-pipeline-bucket"
    gcs_project_id: str = "local-project"
    # e.g. "http://localhost:4443" — handled explicitly in storage/gcs.py
    storage_emulator_host: str = ""

    # Redis
    redis_url: str = "redis://localhost:6379/0"

    # PostgreSQL
    database_url: str = "postgresql+asyncpg://cdc_user:cdc_pass@localhost:5432/cdc_pipeline"

    # PDF source directory (relative to project root)
    pdfs_dir: str = "pdfs"


settings = Settings()

# The google-cloud-pubsub library reads PUBSUB_EMULATOR_HOST directly from
# os.environ at client-instantiation time. pydantic-settings reads .env but
# does NOT populate os.environ, so we sync it here.
if settings.pubsub_emulator_host and "PUBSUB_EMULATOR_HOST" not in os.environ:
    os.environ["PUBSUB_EMULATOR_HOST"] = settings.pubsub_emulator_host
