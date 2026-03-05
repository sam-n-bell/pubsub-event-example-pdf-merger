"""
PDF operations — collecting source files and merging them.

Kept separate from task orchestration so the logic is independently testable
and reusable across tasks or other entry points.
"""

from io import BytesIO
from pathlib import Path

from pypdf import PdfReader, PdfWriter

from event_driven_pdf_pipeline.log import get_logger

logger = get_logger(__name__)


def collect_pdfs(source_dir: str | Path) -> list[Path]:
    """Return a sorted list of all .pdf files found in source_dir."""
    return sorted(Path(source_dir).glob("*.pdf"))


def merge_pdfs(pdf_paths: list[Path]) -> bytes:
    """
    Merge the given PDF files in order and return the combined PDF as bytes.

    Raises ValueError if pdf_paths is empty.
    """
    if not pdf_paths:
        raise ValueError("merge_pdfs requires at least one PDF path")

    writer = PdfWriter()
    for path in pdf_paths:
        logger.debug("appending_pdf", path=str(path))
        writer.append(str(path))

    buf = BytesIO()
    writer.write(buf)
    return buf.getvalue()


def merge_pdfs_from_bytes(pdf_bytes_list: list[bytes]) -> bytes:
    """
    Merge PDF documents supplied as raw bytes (e.g. fetched from a database)
    and return the combined PDF as bytes.

    Raises ValueError if pdf_bytes_list is empty.
    """
    if not pdf_bytes_list:
        raise ValueError("merge_pdfs_from_bytes requires at least one PDF")

    writer = PdfWriter()
    for i, pdf_bytes in enumerate(pdf_bytes_list):
        logger.debug("appending_pdf_bytes", index=i + 1, total=len(pdf_bytes_list))
        writer.append(PdfReader(BytesIO(pdf_bytes)))

    buf = BytesIO()
    writer.write(buf)
    return buf.getvalue()
