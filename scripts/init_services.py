"""
One-time setup script: creates the Pub/Sub topic + subscription and GCS bucket
on the local emulators.

Run after `docker compose up -d` and once all services are healthy:

    uv run python scripts/init_services.py
"""

import asyncio
import sys

from google.auth.credentials import AnonymousCredentials
from google.cloud import pubsub_v1, storage

# config import sets PUBSUB_EMULATOR_HOST in os.environ from .env
from cdc_pdf_pipeline.config import settings
from cdc_pdf_pipeline.db.ops import create_tables
from cdc_pdf_pipeline.log import configure_logging, get_logger

configure_logging()
logger = get_logger(__name__)


def init_pubsub() -> None:
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(settings.pubsub_project_id, settings.pubsub_topic_id)
    try:
        publisher.create_topic(request={"name": topic_path})
        logger.info("topic_created", topic=topic_path)
    except Exception as exc:
        logger.warning("topic_already_exists", error=str(exc))

    subscriber = pubsub_v1.SubscriberClient()
    sub_path = subscriber.subscription_path(
        settings.pubsub_project_id, settings.pubsub_subscription_id
    )
    try:
        subscriber.create_subscription(request={"name": sub_path, "topic": topic_path})
        logger.info("subscription_created", subscription=sub_path)
    except Exception as exc:
        logger.warning("subscription_already_exists", error=str(exc))


def init_gcs() -> None:
    if not settings.storage_emulator_host:
        logger.info("gcs_bucket_skipped", reason="STORAGE_EMULATOR_HOST not set")
        return

    client = storage.Client(
        credentials=AnonymousCredentials(),
        project=settings.gcs_project_id,
        client_options={"api_endpoint": settings.storage_emulator_host},
    )
    try:
        client.create_bucket(settings.gcs_bucket_name)
        logger.info("gcs_bucket_created", bucket=settings.gcs_bucket_name)
    except Exception as exc:
        logger.warning("gcs_bucket_already_exists", error=str(exc))


if __name__ == "__main__":
    logger.info("init_starting")
    init_pubsub()
    init_gcs()
    asyncio.run(create_tables())
    logger.info("init_complete")
    sys.exit(0)
