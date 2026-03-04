import logging

from cdc_pdf_pipeline.broker import broker
from cdc_pdf_pipeline.config import settings
from cdc_pdf_pipeline.gcs import upload_bytes
from cdc_pdf_pipeline.pdf import collect_pdfs, merge_pdfs

logger = logging.getLogger(__name__)


@broker.task
async def merge_and_upload_pdfs(account_id: str, document_type: str) -> str:
    """
    Collect PDFs from the source directory, merge them, and upload to GCS.

    GCS path: {account_id}/{document_type}.pdf

    In production, replace collect_pdfs() with a DB query that fetches the
    blobs for the four rows belonging to this record.
    """
    pdf_paths = collect_pdfs(settings.pdfs_dir)

    if not pdf_paths:
        logger.warning("No PDF files found in %s — skipping merge", settings.pdfs_dir)
        return ""

    logger.info(
        "Merging %d PDF(s) for account_id=%s document_type=%s",
        len(pdf_paths),
        account_id,
        document_type,
    )

    merged_bytes = merge_pdfs(pdf_paths)
    blob_name = f"{account_id}/{document_type}.pdf"
    upload_bytes(blob_name, merged_bytes, content_type="application/pdf")
    logger.info("Uploaded merged PDF → gs://%s/%s", settings.gcs_bucket_name, blob_name)
    return blob_name


@broker.task
async def handle_record_deletion(table: str, record_id: str) -> None:
    """
    Handle a DELETE CDC event.

    In production, resolve which GCS objects belong to this record and remove
    them. Here we write a tombstone object as a demo.
    """
    logger.info("DELETE event received — table=%s record_id=%s", table, record_id)

    blob_name = f"deleted/{table}/{record_id}.tombstone"
    try:
        upload_bytes(
            blob_name,
            f"Deleted record {record_id} from {table}".encode(),
            content_type="text/plain",
        )
        logger.info("Wrote deletion tombstone → gs://%s/%s", settings.gcs_bucket_name, blob_name)
    except Exception as exc:
        logger.error("Failed to write deletion tombstone: %s", exc)
