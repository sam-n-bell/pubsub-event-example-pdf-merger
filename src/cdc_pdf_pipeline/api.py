import asyncio
import io
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse

from cdc_pdf_pipeline.config import settings
from cdc_pdf_pipeline.gcs import download_blob
from cdc_pdf_pipeline.log import configure_logging, get_logger

logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None]:
    configure_logging()
    yield


app = FastAPI(
    title="CDC PDF Pipeline API",
    description="Fetch merged PDFs from GCS by account_id and document_type.",
    lifespan=lifespan,
)


@app.get("/health")
async def health() -> dict:
    return {"status": "ok"}


@app.get(
    "/documents/{account_id}/{document_type}",
    response_class=StreamingResponse,
    responses={
        200: {"content": {"application/pdf": {}}, "description": "The merged PDF"},
        404: {"description": "Document not found in GCS"},
    },
)
async def get_document(account_id: str, document_type: str) -> StreamingResponse:
    """
    Compute the GCS blob path from the two path params and stream the PDF back.

    Path convention (mirrors what the worker uploads):
        {account_id}/{document_type}.pdf
    """
    blob_name = f"{account_id}/{document_type}.pdf"

    # google-cloud-storage is sync-only; offload to a thread to avoid
    # blocking the event loop.
    try:
        data = await asyncio.to_thread(download_blob, blob_name)
    except Exception as exc:
        logger.error("gcs_fetch_failed", blob=blob_name, error=str(exc), exc_info=True)
        raise HTTPException(
            status_code=500, detail="Failed to fetch document from storage"
        ) from exc

    if data is None:
        raise HTTPException(
            status_code=404,
            detail=f"No document found at gs://{settings.gcs_bucket_name}/{blob_name}",
        )

    return StreamingResponse(
        io.BytesIO(data),
        media_type="application/pdf",
        headers={
            "Content-Disposition": f'inline; filename="{document_type}.pdf"',
            "Content-Length": str(len(data)),
        },
    )
