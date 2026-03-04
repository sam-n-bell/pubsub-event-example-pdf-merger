"""
Database layer for the pdf_documents table.

Oracle BLOB maps to PostgreSQL BYTEA via GoldenGate — that's the column type
used here, represented in SQLAlchemy as LargeBinary.

Session management follows the async generator pattern:

    # FastAPI dependency injection:
    async def endpoint(session: AsyncSession = Depends(get_session)): ...

    # Everywhere else (tasks, scripts):
    async with AsyncSessionLocal() as session:
        pdfs = await fetch_all_pdfs(session)
"""

from collections.abc import AsyncGenerator

from sqlalchemy import LargeBinary, Text, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from cdc_pdf_pipeline.config import settings
from cdc_pdf_pipeline.log import get_logger

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Engine & session factory
# ---------------------------------------------------------------------------

engine = create_async_engine(settings.database_url, echo=False)

AsyncSessionLocal = async_sessionmaker(engine, expire_on_commit=False)


# ---------------------------------------------------------------------------
# ORM model
# ---------------------------------------------------------------------------


class Base(DeclarativeBase):
    pass


class PdfDocument(Base):
    """One row per source PDF.  BYTEA ← Oracle BLOB via GoldenGate."""

    __tablename__ = "pdf_documents"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    filename: Mapped[str] = mapped_column(Text, unique=True, nullable=False)
    data: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)


# ---------------------------------------------------------------------------
# Session generator
# ---------------------------------------------------------------------------


async def get_session() -> AsyncGenerator[AsyncSession]:
    """
    Async generator that yields a database session and closes it on exit.

    Use as a FastAPI dependency (Depends) or as an async context manager
    via AsyncSessionLocal() for non-FastAPI callers.
    """
    async with AsyncSessionLocal() as session:
        yield session


# ---------------------------------------------------------------------------
# Operations
# ---------------------------------------------------------------------------


async def create_tables() -> None:
    """Create all ORM-mapped tables if they do not already exist."""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("db_tables_ready")


async def fetch_all_pdfs(session: AsyncSession) -> list[bytes]:
    """
    Return raw PDF bytes for every row in pdf_documents, ordered by id.

    In production, add a filter on account_id / document_type (or whichever
    columns GoldenGate replicates from the Oracle source table).
    """
    result = await session.execute(select(PdfDocument).order_by(PdfDocument.id))
    return [doc.data for doc in result.scalars().all()]


async def upsert_pdf(session: AsyncSession, filename: str, data: bytes) -> None:
    """Insert or replace a PDF row identified by filename."""
    stmt = (
        pg_insert(PdfDocument)
        .values(filename=filename, data=data)
        .on_conflict_do_update(
            index_elements=["filename"],
            set_={"data": data},
        )
    )
    await session.execute(stmt)
    await session.commit()
    logger.info("pdf_upserted", filename=filename, bytes=len(data))
