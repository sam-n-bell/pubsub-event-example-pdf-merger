"""
Pub/Sub subscriber that listens for events and dispatches taskiq tasks.

All incoming messages are validated against CdcEvent (messaging/schemas.py).
Malformed messages are nack'd and logged immediately.

Routing:
  I (INSERT) / U (UPDATE) on PDF_TABLES → merge_and_upload_pdfs
  D (DELETE) on any table               → handle_record_deletion
"""

import asyncio
import signal

from google.cloud import pubsub_v1
from google.cloud.pubsub_v1.subscriber.message import Message
from pydantic import ValidationError

# config import also ensures PUBSUB_EMULATOR_HOST is set in os.environ
from event_driven_pdf_pipeline.config import settings
from event_driven_pdf_pipeline.log import configure_logging, get_logger
from event_driven_pdf_pipeline.messaging.schemas import CdcEvent, OpType
from event_driven_pdf_pipeline.messaging.tasks import handle_record_deletion, merge_and_upload_pdfs

logger = get_logger(__name__)

# Tables whose INSERT/UPDATE events trigger a PDF merge
PDF_TABLES = {"documents", "attachments", "files"}


def _dispatch(coro) -> None:
    """Run an async taskiq .kiq() coroutine from a sync Pub/Sub callback thread."""
    asyncio.run(coro)


def process_message(message: Message) -> None:
    try:
        event = CdcEvent.model_validate_json(message.data.decode("utf-8"))
    except (ValidationError, UnicodeDecodeError) as exc:
        logger.error("message_validation_failed", error=str(exc))
        message.nack()
        return

    logger.info("cdc_event_received", table=event.table, op_type=event.op_type)

    try:
        if event.op_type == OpType.DELETE:
            # DELETE carries the deleted row in `before`
            record_id = event.before.id if event.before else "unknown"
            _dispatch(handle_record_deletion.kiq(table=event.table, record_id=record_id))
            logger.info(
                "task_enqueued",
                task="handle_record_deletion",
                table=event.table,
                pk=record_id,
            )

        elif event.op_type in (OpType.INSERT, OpType.UPDATE) and event.table in PDF_TABLES:
            # INSERT / UPDATE carry the new row in `after`
            after = event.after
            if after is None or not after.account_id or not after.document_type:
                logger.warning(
                    "incomplete_event",
                    missing="after.account_id or after.document_type",
                    after=after.model_dump() if after else None,
                )
            else:
                _dispatch(
                    merge_and_upload_pdfs.kiq(
                        account_id=after.account_id,
                        document_type=after.document_type,
                    )
                )
                logger.info(
                    "task_enqueued",
                    task="merge_and_upload_pdfs",
                    account_id=after.account_id,
                    document_type=after.document_type,
                )
        else:
            logger.debug("no_task_mapped", table=event.table, op_type=event.op_type)

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
