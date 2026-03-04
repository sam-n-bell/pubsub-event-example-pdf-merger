"""
Pub/Sub subscriber that listens for CDC events and dispatches taskiq tasks.

CDC event JSON shape:
{
    "table": "documents",        # documents | accounts | metadata
    "operation": "INSERT",       # INSERT | UPDATE | DELETE
    "data": {
        "pk": "rec-001",
        "account_id": "ACC-001",
        "document_type": "contract"
    }
}

Routing:
  INSERT / UPDATE on "documents" → merge_and_upload_pdfs
  DELETE  on any table           → handle_record_deletion
"""

import asyncio
import json
import signal

from google.cloud import pubsub_v1

# config import also ensures PUBSUB_EMULATOR_HOST is set in os.environ
from cdc_pdf_pipeline.config import settings
from cdc_pdf_pipeline.log import configure_logging, get_logger
from cdc_pdf_pipeline.tasks import handle_record_deletion, merge_and_upload_pdfs

logger = get_logger(__name__)

# Tables whose INSERT/UPDATE events trigger a PDF merge
PDF_TABLES = {"documents", "attachments", "files"}


def _dispatch(coro) -> None:
    """Run an async taskiq .kiq() coroutine from a sync Pub/Sub callback thread."""
    asyncio.run(coro)


def process_message(message: pubsub_v1.subscriber.message.Message) -> None:
    try:
        data = json.loads(message.data.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        logger.error("message_decode_failed", error=str(exc))
        message.nack()
        return

    table = data.get("table", "")
    operation = data.get("operation", "").upper()
    record = data.get("data", {})

    logger.info("cdc_event_received", table=table, operation=operation)

    try:
        if operation == "DELETE":
            record_id = record.get("pk", "unknown")
            _dispatch(handle_record_deletion.kiq(table=table, record_id=record_id))
            logger.info("task_enqueued", task="handle_record_deletion", table=table, pk=record_id)

        elif operation in ("INSERT", "UPDATE") and table in PDF_TABLES:
            account_id = record.get("account_id", "")
            document_type = record.get("document_type", "")
            if not account_id or not document_type:
                logger.warning(
                    "incomplete_event", missing="account_id or document_type", data=record
                )
            else:
                _dispatch(
                    merge_and_upload_pdfs.kiq(account_id=account_id, document_type=document_type)
                )
                logger.info(
                    "task_enqueued",
                    task="merge_and_upload_pdfs",
                    account_id=account_id,
                    document_type=document_type,
                )
        else:
            logger.debug("no_task_mapped", table=table, operation=operation)

        message.ack()

    except Exception as exc:
        logger.error("task_dispatch_failed", error=str(exc), exc_info=True)
        message.nack()


def _run() -> None:
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(
        settings.pubsub_project_id, settings.pubsub_subscription_id
    )

    logger.info("subscriber_starting", subscription=subscription_path)

    with subscriber:
        future = subscriber.subscribe(subscription_path, callback=process_message)
        # Graceful shutdown on Ctrl-C
        signal.signal(signal.SIGINT, lambda *_: future.cancel())
        signal.signal(signal.SIGTERM, lambda *_: future.cancel())
        try:
            future.result()
        except Exception:
            logger.info("subscriber_stopped")


def run() -> None:
    configure_logging()
    _run()


if __name__ == "__main__":
    run()
