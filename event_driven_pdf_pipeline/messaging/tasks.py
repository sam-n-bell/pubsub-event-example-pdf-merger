from event_driven_pdf_pipeline.broker import broker
from event_driven_pdf_pipeline.config import settings
from event_driven_pdf_pipeline.db.ops import fetch_all_pdfs_new_session
from event_driven_pdf_pipeline.log import configure_logging, get_logger
from event_driven_pdf_pipeline.storage.gcs import upload_bytes
from event_driven_pdf_pipeline.storage.pdf import merge_pdfs_from_bytes

logger = get_logger(__name__)

# Configure logging at import time so the worker process gets structured logs
# as soon as it loads this module. configure_logging() is idempotent.
configure_logging()


@broker.task
async def merge_and_upload_pdfs(account_id: str, document_type: str) -> str:
    """
    Fetch PDF rows from the database, merge them, and upload to GCS.

    GCS path: {account_id}/{document_type}.pdf

    In production, filter fetch_all_pdfs() by account_id / document_type
    (or whatever key GoldenGate replicates from the source Oracle table).
    """
    pdf_bytes_list = await fetch_all_pdfs_new_session()

    if not pdf_bytes_list:
        logger.warning("no_pdfs_in_db")
        return ""

    logger.info(
        "merging_pdfs",
        count=len(pdf_bytes_list),
        account_id=account_id,
        document_type=document_type,
    )

    merged_bytes = merge_pdfs_from_bytes(pdf_bytes_list)
    blob_name = f"{account_id}/{document_type}.pdf"
    upload_bytes(blob_name, merged_bytes, content_type="application/pdf")
    logger.info("pdf_uploaded", bucket=settings.gcs_bucket_name, blob=blob_name)
    return blob_name


@broker.task
async def handle_record_deletion(table: str, record_id: str) -> None:
    """
    Handle a DELETE CDC event.

    In production, resolve which GCS objects belong to this record and remove
    them. Here we write a tombstone object as a demo.
    """
    logger.info("delete_event_received", table=table, record_id=record_id)

    blob_name = f"deleted/{table}/{record_id}.tombstone"
    try:
        upload_bytes(
            blob_name,
            f"Deleted record {record_id} from {table}".encode(),
            content_type="text/plain",
        )
        logger.info("tombstone_written", bucket=settings.gcs_bucket_name, blob=blob_name)
    except Exception as exc:
        logger.error("tombstone_write_failed", error=str(exc))
