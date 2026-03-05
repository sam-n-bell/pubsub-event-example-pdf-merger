"""
Seed the pdf_documents table from the local pdfs/ directory.

One row is upserted per file (keyed on filename), so re-running is safe.

Usage:
    uv run python scripts/load_pdfs.py
    make load-pdfs
"""

import asyncio
import sys
from pathlib import Path

from cdc_pdf_pipeline.config import settings
from cdc_pdf_pipeline.db.ops import AsyncSessionLocal, create_tables, upsert_pdf
from cdc_pdf_pipeline.log import configure_logging, get_logger
from cdc_pdf_pipeline.storage.pdf import collect_pdfs

configure_logging()
logger = get_logger(__name__)


async def main() -> None:
    pdfs_dir = Path(settings.pdfs_dir)
    pdf_paths = collect_pdfs(pdfs_dir)

    if not pdf_paths:
        logger.warning("no_pdfs_found", dir=str(pdfs_dir))
        sys.exit(0)

    await create_tables()

    async with AsyncSessionLocal() as session:
        for pdf_path in pdf_paths:
            await upsert_pdf(session, pdf_path.name, pdf_path.read_bytes())

    logger.info("load_complete", count=len(pdf_paths))


if __name__ == "__main__":
    asyncio.run(main())
