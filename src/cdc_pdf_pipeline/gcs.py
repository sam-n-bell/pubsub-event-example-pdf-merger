from typing import cast

from google.auth.credentials import AnonymousCredentials
from google.cloud import storage

from cdc_pdf_pipeline.config import settings


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


def download_blob(blob_name: str) -> bytes | None:
    """
    Download a GCS blob as bytes.
    Returns None if the blob does not exist.
    """
    client = get_storage_client()
    bucket = client.bucket(settings.gcs_bucket_name)
    blob = bucket.blob(blob_name)
    if not blob.exists():
        return None
    return cast(bytes, blob.download_as_bytes())
