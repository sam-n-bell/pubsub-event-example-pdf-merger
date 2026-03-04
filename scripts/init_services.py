"""
One-time setup script: creates the Pub/Sub topic + subscription and GCS bucket
on the local emulators.

Run after `docker compose up -d` and once all services are healthy:

    uv run python scripts/init_services.py
"""

import logging
import sys

# config import sets PUBSUB_EMULATOR_HOST in os.environ from .env
from cdc_pdf_pipeline.config import settings
from google.auth.credentials import AnonymousCredentials
from google.cloud import pubsub_v1, storage

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
)
logger = logging.getLogger(__name__)


def init_pubsub() -> None:
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(settings.pubsub_project_id, settings.pubsub_topic_id)
    try:
        publisher.create_topic(request={"name": topic_path})
        logger.info("Created topic:        %s", topic_path)
    except Exception as exc:
        logger.warning("Topic already exists (or error): %s", exc)

    subscriber = pubsub_v1.SubscriberClient()
    sub_path = subscriber.subscription_path(
        settings.pubsub_project_id, settings.pubsub_subscription_id
    )
    try:
        subscriber.create_subscription(request={"name": sub_path, "topic": topic_path})
        logger.info("Created subscription: %s", sub_path)
    except Exception as exc:
        logger.warning("Subscription already exists (or error): %s", exc)


def init_gcs() -> None:
    if not settings.storage_emulator_host:
        logger.info("STORAGE_EMULATOR_HOST not set — skipping GCS bucket creation")
        return

    client = storage.Client(
        credentials=AnonymousCredentials(),
        project=settings.gcs_project_id,
        client_options={"api_endpoint": settings.storage_emulator_host},
    )
    try:
        client.create_bucket(settings.gcs_bucket_name)
        logger.info("Created GCS bucket:   %s", settings.gcs_bucket_name)
    except Exception as exc:
        logger.warning("Bucket already exists (or error): %s", exc)


if __name__ == "__main__":
    logger.info("Initialising emulator services…")
    init_pubsub()
    init_gcs()
    logger.info("Done.")
    sys.exit(0)
