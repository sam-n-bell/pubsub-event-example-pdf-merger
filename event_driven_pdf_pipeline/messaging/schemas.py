"""
Pydantic schema for GoldenGate CDC events published to the Pub/Sub topic.

Reflects the Oracle GoldenGate JSON operation-based formatter output.
Every message on the topic must conform to CdcEvent. Both the publisher
(outbound) and subscriber (inbound validation) use this schema.

Schema shape:

{
  "type": "record",
  "name": "CdcEvent",
  "fields": [
    { "name": "table",        "type": "string" },
    { "name": "op_type",      "type": "string" },
    { "name": "op_ts",        "type": "string", "default": "" },
    { "name": "current_ts",   "type": "string", "default": "" },
    { "name": "pos",          "type": "string", "default": "" },
    { "name": "primary_keys", "type": { "type": "array", "items": "string" }, "default": [] },
    { "name": "before", "type": [ "null", { "type": "map", "values": "string" } ], "default": null },
    { "name": "after",  "type": [ "null", { "type": "map", "values": "string" } ], "default": null }
  ]
}


JSON shape:
{
    "table": "SCHEMA.DOCUMENTS",
    "op_type": "I",
    "op_ts": "2024-01-15 10:23:45.000000",
    "current_ts": "2024-01-15 10:23:45.123000",
    "pos": "00000000000000001234",
    "primary_keys": ["id"],
    "before": null,
    "after": {
        "id": "rec-001",
        "account_id": "ACC-001",
        "document_type": "contract"
    }
}

op_type values:
  I – INSERT  (before=null, after=row)
  U – UPDATE  (before=row,  after=row)
  D – DELETE  (before=row,  after=null)

The validator also accepts long-form strings (INSERT / UPDATE / DELETE)
so the mock publisher CLI remains ergonomic.
"""

from enum import StrEnum

from pydantic import BaseModel, Field, field_validator

_LONG_TO_SHORT: dict[str, str] = {
    "INSERT": "I",
    "UPDATE": "U",
    "DELETE": "D",
}


class OpType(StrEnum):
    INSERT = "I"
    UPDATE = "U"
    DELETE = "D"


class CdcRowData(BaseModel):
    """Column values for a single row image (before or after)."""

    id: str = "unknown"
    account_id: str = ""
    document_type: str = ""


class CdcEvent(BaseModel):
    table: str
    op_type: OpType
    op_ts: str = ""
    current_ts: str = ""
    pos: str = ""
    primary_keys: list[str] = Field(default_factory=list)
    before: CdcRowData | None = None
    after: CdcRowData | None = None

    @field_validator("op_type", mode="before")
    @classmethod
    def normalise_op_type(cls, v: object) -> object:
        """Accept both single-char ('I') and long-form ('INSERT')."""
        if isinstance(v, str):
            return _LONG_TO_SHORT.get(v.upper(), v.upper())
        return v
