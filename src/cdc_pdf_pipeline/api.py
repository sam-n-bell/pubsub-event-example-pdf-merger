import asyncio
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from starlette.concurrency import iterate_in_threadpool

from cdc_pdf_pipeline.config import settings
from cdc_pdf_pipeline.gcs import blob_exists, stream_blob
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

    GCS chunks are piped directly to the HTTP response via iterate_in_threadpool,
    so the full file is never buffered in server memory.

    Path convention (mirrors what the worker uploads):
        {account_id}/{document_type}.pdf
    """
    blob_name = f"{account_id}/{document_type}.pdf"

    try:
        exists = await asyncio.to_thread(blob_exists, blob_name)
    except Exception as exc:
        logger.error("gcs_exists_check_failed", blob=blob_name, error=str(exc), exc_info=True)
        raise HTTPException(
            status_code=500, detail="Failed to reach storage"
        ) from exc

    if not exists:
        raise HTTPException(
            status_code=404,
            detail=f"No document found at gs://{settings.gcs_bucket_name}/{blob_name}",
        )

    return StreamingResponse(
        iterate_in_threadpool(stream_blob(blob_name)),
        media_type="application/pdf",
        headers={"Content-Disposition": f'inline; filename="{document_type}.pdf"'},
    )
