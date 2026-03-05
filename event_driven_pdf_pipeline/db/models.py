from sqlalchemy import LargeBinary, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class PdfDocument(Base):
    """One row per source PDF.  BYTEA ← Oracle BLOB via GoldenGate."""

    __tablename__ = "pdf_documents"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    filename: Mapped[str] = mapped_column(Text, unique=True, nullable=False)
    data: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
