"""
PDF operations — collecting source files and merging them.

Kept separate from task orchestration so the logic is independently testable
and reusable across tasks or other entry points.
"""

import logging
from io import BytesIO
from pathlib import Path

from pypdf import PdfWriter

logger = logging.getLogger(__name__)


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
        logger.debug("Appending %s", path)
        writer.append(str(path))

    buf = BytesIO()
    writer.write(buf)
    return buf.getvalue()
