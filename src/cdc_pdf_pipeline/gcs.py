from collections.abc import Iterator
from typing import cast

from google.auth.credentials import AnonymousCredentials
from google.cloud import storage

from cdc_pdf_pipeline.config import settings

_CHUNK_SIZE = 256 * 1024  # 256 KB


def get_storage_client() -> storage.Client:
    """
    Returns a GCS client.

    When STORAGE_EMULATOR_HOST is set (e.g. "http://localhost:4443"), the client
    is pointed at fake-gcs-server using anonymous credentials. In production,
    leave STORAGE_EMULATOR_HOST unset and normal ADC auth applies.
    """
    if settings.storage_emulator_host:
        return storage.Client(
            credentials=AnonymousCredentials(),
            project=settings.gcs_project_id,
            client_options={"api_endpoint": settings.storage_emulator_host},
        )
    return storage.Client()


def upload_bytes(
    blob_name: str,
    data: bytes,
    content_type: str = "application/octet-stream",
) -> None:
    """Upload raw bytes to a GCS blob, overwriting any existing object."""
    client = get_storage_client()
    bucket = client.bucket(settings.gcs_bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_string(data, content_type=content_type)


def blob_exists(blob_name: str) -> bool:
    """Return True if the blob exists in the configured bucket."""
    client = get_storage_client()
    bucket = client.bucket(settings.gcs_bucket_name)
    return cast(bool, bucket.blob(blob_name).exists())


def stream_blob(blob_name: str) -> Iterator[bytes]:
    """
    Yield the blob's contents in _CHUNK_SIZE chunks.

    Assumes the blob exists — call blob_exists() first.
    The GCS client is sync-only so this is a regular (non-async) generator;
    wrap with iterate_in_threadpool when using inside an async context.
    """
    client = get_storage_client()
    bucket = client.bucket(settings.gcs_bucket_name)
    with bucket.blob(blob_name).open("rb") as f:
        while chunk := f.read(_CHUNK_SIZE):
            yield chunk
